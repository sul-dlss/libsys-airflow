import csv
import logging
import pathlib
import json

import pymarc
import requests

from airflow.models import Variable

from migration_tools.library_configuration import LibraryConfiguration
from migration_tools.migration_tasks.bibs_transformer import BibsTransformer



logger = logging.getLogger(__name__)

sul_config = LibraryConfiguration(
    okapi_url=Variable.get("OKAPI_URL"),
    tenant_id="sul",
    okapi_username=Variable.get("FOLIO_USER"),
    okapi_password=Variable.get("FOLIO_PASSWORD"),
    library_name="Stanford University Libraries",
    base_folder="/opt/airflow/migration",
    log_level_debug=True,
    folio_release="juniper",
    iteration_identifier="",
)


def _get_files(files: list) -> list:
    output = []
    for row in files:
        file_name = row.split("/")[-1]
        output.append({"file_name": file_name, "suppressed": False})
    return output


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


def preprocess_marc(*args, **kwargs):
    for path in pathlib.Path(f"{airflow}/symphony/").glob("*.*rc"):
        marc_records = []
        marc_reader = pymarc.MARCReader(path.read_bytes())
        for record in marc_reader:
            _move_001_to_035(record)
            marc_records.append(record)
        with open(path.absolute(), "wb+") as fo:
            marc_writer = pymarc.MARCWriter(fo)
            for record in marc_records:
                marc_writer.write(record)
    holdings_tsv_file.close()


def process_records(*args, **kwargs) -> list:
    """Function creates valid json from file of FOLIO objects"""
    pattern = kwargs.get("pattern")
    out_filename = kwargs.get("out_filename")

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


def run_bibs_transformer(*args, **kwargs):
    task_instance = kwargs["task_instance"]
    dag = kwargs["dag_run"]

    files = _get_files(
        task_instance.xcom_pull(key="return_value", task_ids="move_marc_files")
    )

    sul_config.iteration_identifier = dag.run_id

    bibs_configuration = BibsTransformer.TaskConfiguration(
        name="bibs-transformer",
        migration_task_type="BibsTransformer",
        hrid_handling="default",
        files=files,
        ils_flavour="voyager",  # Voyager uses 001 field, using tag001 works
    )

    bibs_transformer = BibsTransformer(
        bibs_configuration, sul_config, use_logging=False
    )

    logger.info(f"Starting bibs_tranfers work for {files}")

    bibs_transformer.do_work()

    bibs_transformer.wrap_up()


def folio_login(**kwargs):
    """Logs into FOLIO and returns Okapi token."""
    okapi_url = Variable.get("OKAPI_URL")
    username = Variable.get("FOLIO_USER")
    password = Variable.get("FOLIO_PASSWORD")
    tenant = "sul"

    data = {"username": username, "password": password}
    headers = {"Content-type": "application/json", "x-okapi-tenant": tenant}

    url = f"{okapi_url}/authn/login"
    result = requests.post(url, json=data, headers=headers)

    if result.status_code == 201:  # Valid token created and returned
        return result.headers.get("x-okapi-token")

    result.raise_for_status()


def _post_to_okapi(**kwargs):
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


def post_folio_instance_records(**kwargs):
    """Creates new records in FOLIO"""

    batch_size = kwargs.get("MAX_ENTITIES", 1000)
    job_number = kwargs.get("job")

    with open(f"/tmp/instances-{job_number}.json") as fo:
        instance_records = json.load(fo)

    for i in range(0, len(instance_records), batch_size):
        instance_batch = instance_records[i: i + batch_size]
        logger.info(f"Posting {len(instance_batch)} in batch {i}")
        _post_to_okapi(
            token=kwargs["task_instance"].xcom_pull(
                key="return_value", task_ids="post-to-folio.folio_login"
            ),
            records=instance_batch,
            endpoint="/instance-storage/batch/synchronous?upsert=true",
            payload_key="instances",
            **kwargs,
        )


def post_folio_holding_records(**kwargs):
    """Creates/overlays Holdings records in FOLIO"""

    batch_size = kwargs.get("MAX_ENTITIES", 1000)
    job_number = kwargs.get("job")

    with open(f"/tmp/holdings-{job_number}.json") as fo:
        holding_records = json.load(fo)

    for i in range(0, len(holding_records), batch_size):
        holdings_batch = holding_records[i: i + batch_size]
        logger.info(f"Posting {i} to {i+batch_size} holding records")
        _post_to_okapi(
            token=kwargs["task_instance"].xcom_pull(
                key="return_value", task_ids="post-to-folio.folio_login"
            ),
            records=holdings_batch,
            endpoint="/holdings-storage/batch/synchronous?upsert=true",
            payload_key="holdingsRecords",
            **kwargs,
        )
