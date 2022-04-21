import json
import logging
import pathlib
import shutil

import pandas as pd
import pymarc
import requests

from airflow.models import Variable
from folio_migration_tools.migration_tasks.migration_task_base import LevelFilter

logger = logging.getLogger(__name__)


def archive_artifacts(*args, **kwargs):
    """Archives JSON Instances, Items, and Holdings"""
    dag = kwargs["dag_run"]

    airflow = kwargs.get("airflow", "/opt/airflow")
    tmp = kwargs.get("tmp_dir", "/tmp")

    airflow_path = pathlib.Path(airflow)
    tmp_path = pathlib.Path(tmp)

    airflow_results = airflow_path / "migration/results"
    archive_directory = airflow_path / "migration/archive"

    for tmp_file in tmp_path.glob('*.json'):
        try:
            tmp_file.unlink()
        except OSError as err:
            logger.info(f"Cannot remove {tmp_file}: {err}")

    for artifact in airflow_results.glob(f"*{dag.run_id}*.json"):

        target = archive_directory / artifact.name

        shutil.move(artifact, target)
        logger.info("Moved {artifact} to {target}")


def move_marc_files_check_tsv(*args, **kwargs) -> str:
    """Moves MARC files to migration/data/instances, sets XCOM
    if tsv is present"""
    task_instance = kwargs["task_instance"]

    airflow = kwargs.get("airflow", "/opt/airflow")
    source_directory = kwargs["source"]

    marc_path = next(
        pathlib.Path(f"{airflow}/{source_directory}/").glob("*.*rc")
    )
    if not marc_path.exists():
        raise ValueError(f"MARC Path {marc_path} does not exist")

    # Checks for TSV file and sets XCOM marc_only if not present
    tsv_path = pathlib.Path(
        f"{airflow}/{source_directory}/{marc_path.stem}.tsv"
    )
    marc_only = True
    if tsv_path.exists():
        marc_only = False
    task_instance.xcom_push(key="marc_only", value=marc_only)

    marc_target = pathlib.Path(
        f"{airflow}/migration/data/instances/{marc_path.name}"
    )
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

    marc_path = pathlib.Path(
        f"/opt/airflow/migration/data/instances/{marc_stem}.mrc"
    )
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


def _save_error_record_ids(**kwargs):
    dag = kwargs["dag_run"]
    records = kwargs["records"]
    endpoint = kwargs["endpoint"]
    error_code = kwargs["error_code"]
    airflow = kwargs.get("airflow", pathlib.Path("/opt/airflow/"))

    record_base = endpoint.split("/")[1]

    error_filepath = (
        airflow
        / "migration/results"
        / f"errors-{record_base}-{error_code}-{dag.run_id}.json"
    )

    with error_filepath.open('a') as error_file:
        for rec in records:
            error_file.write(json.dumps(rec))
            error_file.write("\n")


def post_to_okapi(**kwargs) -> bool:
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
        _save_error_record_ids(
            error_code=new_record_result.status_code,
            **kwargs
        )


def process_records(*args, **kwargs) -> list:
    """Function creates valid json from file of FOLIO objects"""
    prefix = kwargs.get("prefix")
    dag = kwargs["dag_run"]

    pattern = f"{prefix}*{dag.run_id}*.json"

    out_filename = f"{kwargs.get('out_filename')}-{dag.run_id}"

    total_jobs = int(kwargs.get("jobs"))

    airflow = kwargs.get("airflow", "/opt/airflow")
    tmp = kwargs.get("tmp", "/tmp")

    records = []
    for file in pathlib.Path(f"{airflow}/migration/results").glob(pattern):
        with open(file) as fo:
            records.extend([json.loads(i) for i in fo.readlines()])

    shard_size = int(len(records) / total_jobs)
    for i in range(total_jobs):
        start = i * shard_size
        end = int(start + shard_size)
        if end >= len(records):
            end = None
        logger.error(f"Start {start} End {end}")
        with open(f"{tmp}/{out_filename}-{i}.json", "w+") as fo:
            json.dump(records[start:end], fo)

    return len(records)


def setup_data_logging(transformer):
    def transformer_data_issues(transformer, message, *args, **kwargs):
        transformer._log(DATA_ISSUE_LVL_NUM, message, args, **kwargs)

    # Set DATA_ISSUE logging levels
    DATA_ISSUE_LVL_NUM = 26
    logging.addLevelName(DATA_ISSUE_LVL_NUM, "DATA_ISSUES")
    logging.Logger.data_issues = transformer_data_issues

    data_issue_file_formatter = logging.Formatter("%(message)s")
    data_issue_file_handler = logging.FileHandler(
        filename=str(transformer.folder_structure.data_issue_file_path),
    )
    data_issue_file_handler.addFilter(LevelFilter(26))
    data_issue_file_handler.setFormatter(data_issue_file_formatter)
    data_issue_file_handler.setLevel(26)
    logging.getLogger().addHandler(data_issue_file_handler)


def _apply_processing_tsv(tsv_path, airflow, column_transforms):
    df = pd.read_csv(tsv_path, sep="\t")
    # Performs any transformations to values
    for transform in column_transforms:
        column = transform[0]
        if column in df:
            function = transform[1]
            df[column] = df[column].apply(function)
    new_tsv_path = pathlib.Path(f"{airflow}/migration/data/items/{tsv_path.name}")
    df.to_csv(new_tsv_path, sep="\t", index=False)
    tsv_path.unlink()


def _get_tsv(**kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    source_directory = kwargs["source"]

    return [path for path in pathlib.Path(f"{airflow}/{source_directory}/").glob("*.tsv")]


def transform_move_tsvs(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    column_transforms = kwargs.get("column_transforms", [])

    tsv_paths = _get_tsv(**kwargs)

    path_names = [f"{path.name}.tsv" for path in tsv_paths]

    if len(tsv_paths) < 1:
        raise ValueError(
            "No csv files exist for workflow"
        )

    for path in tsv_paths:
        _apply_processing_tsv(path, airflow, column_transforms)

    return path_names
