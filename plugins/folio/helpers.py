import json
import logging
import pathlib
import shutil

import pandas as pd
import pymarc
import requests

logger = logging.getLogger(__name__)

from airflow.models import Variable


def archive_artifacts(*args, **kwargs):
    """Archives JSON Instances, Items, and Holdings"""
    dag = kwargs["dag_run"]

    airflow = kwargs.get("airflow", "/opt/airflow")
    airflow_path = pathlib.Path(airflow)

    archive_directory = airflow_path / "migration/archive"

    for artifact in airflow_path.glob(f"*-{dag.run_id}*.json"):
        target = archive_directory / artifact.name

        shutil.move(artifact, target)
        logger.info("Moved {artifact} to {target}")


def move_marc_files_check_csv(*args, **kwargs) -> str:
    """Moves MARC files to migration/data/instances, sets XCOM if csv is present"""
    task_instance = kwargs["task_instance"]

    airflow = kwargs.get("airflow", "/opt/airflow")
    source_directory = kwargs["source"]

    marc_path = next(pathlib.Path(f"{airflow}/{source_directory}/").glob("*.*rc"))
    if not marc_path.exists():
        raise ValueError(f"MARC Path {marc_path} does not exist")

    # Checks for CSV file and sets XCOM marc_only if not present
    csv_path = pathlib.Path(f"{airflow}/{source_directory}/{marc_path.stem}.csv")
    marc_only = True
    if csv_path.exists():
        marc_only = False
    task_instance.xcom_push(key="marc_only", value=marc_only)

    marc_target = pathlib.Path(f"{airflow}/migration/data/instances/{marc_path.name}")
    shutil.move(marc_path, marc_target)


    return marc_path.stem


def _move_001_to_035(record: pymarc.Record):
    all001 = record.get_fields("001")
    if len(all001) < 2:
        return
    for field001 in all001[1:]:
        field035 = pymarc.Field(
            tag="035", indicators=["", ""], subfields=["a", field001.data]
        )
        record.add_field(field035)
        record.remove_field(field001)


def process_marc(*args, **kwargs):
    marc_stem = kwargs["marc_stem"]

    marc_path = pathlib.Path(f"/opt/airflow/migration/data/instances/{marc_stem}.mrc")
    marc_reader = pymarc.MARCReader(marc_path.read_bytes())

    marc_records = []

    for record in marc_reader:
        _move_001_to_035(record)
        marc_records.append(record)
        count = len(marc_records)
        if not count % 10000:
            logger.info(f"Processed {count} MARC records")

    with open(marc_path.absolute(), "wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for i, record in enumerate(marc_records):
            marc_writer.write(record)
            if not i % 10000:
                logger.info(f"Writing record {i}")


def post_to_okapi(**kwargs):
    endpoint = kwargs.get("endpoint")
    jwt = kwargs["token"]

    records = kwargs["records"]
    payload_key = kwargs["payload_key"]

    tenant = "sul"
    okapi_url = Variable.get("OKAPI_URL")

    okapi_instance_url = f"{okapi_url}{endpoint}"

    headers = {
        "Content-type": "application/json",
        "user-agent": "FolioAirflow",
        "x-okapi-token": jwt,
        "x-okapi-tenant": tenant,
    }

    payload = {payload_key: records}

    new_record_result = requests.post(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    logger.info(
        f"Result status code {new_record_result.status_code} for {len(records)} records"  # noqa
    )

    if new_record_result.status_code > 399:
        logger.error(new_record_result.text)
        raise ValueError(
            f"FOLIO POST Failed with error code:{new_record_result.status_code}"  # noqa
        )


def process_records(*args, **kwargs) -> list:
    """Function creates valid json from file of FOLIO objects"""
    prefix = kwargs.get("prefix")
    dag = kwargs["dag_run"]

    pattern = f"{prefix}*{dag.run_id}*.json"

    out_filename = f"{kwargs.get('out_filename')}-{dag.run_id}"

    total_jobs = int(kwargs.get("jobs"))

    records = []
    for file in pathlib.Path("/opt/airflow/migration/results").glob(pattern):
        with open(file) as fo:
            records.extend([json.loads(i) for i in fo.readlines()])

    shard_size = int(len(records) / total_jobs)
    for i in range(total_jobs):
        start = i * shard_size
        end = int(start + shard_size)
        if end >= len(records):
            end = None
        logger.error(f"Start {start} End {end}")
        with open(f"/tmp/{out_filename}-{i}.json", "w+") as fo:
            json.dump(records[start:end], fo)

    return len(records)


def tranform_csv_to_tsv(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    marc_stem = kwargs["marc_stem"]
    column_names = kwargs["column_names"]
    column_transforms = kwargs.get("column_transforms", [])
    source_directory = kwargs["source"]

    csv_path = pathlib.Path(f"{airflow}/{source_directory}/{marc_stem}.csv")
    if not csv_path.exists():
        raise ValueError(f"CSV Path {csv_path} does not exist for {marc_stem}.mrc")
    df = pd.read_csv(csv_path, names=column_names)

    # Performs any transformations to values
    for transform in column_transforms:
        column = transform[0]
        function = transform[1]
        df[column] = df[column].apply(function)
    tsv_path = pathlib.Path(f"{airflow}/migration/data/items/{marc_stem}.tsv")
    df.to_csv(tsv_path, sep="\t", index=False)

    csv_path.unlink()
    return tsv_path.name
