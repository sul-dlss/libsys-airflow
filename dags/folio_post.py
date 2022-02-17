import csv
import logging
import pathlib
import json

import pymarc
import requests

from airflow.models import Variable

from plugins.folio.helpers import post_to_okapi as _post_to_okapi

from migration_tools.migration_tasks.bibs_transformer import BibsTransformer

logger = logging.getLogger(__name__)


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


def run_bibs_transformer(*args, **kwargs):
    dag = kwargs["dag_run"]

    library_config = kwargs["library_config"]

    marc_stem = kwargs["marc_stem"]
    
    library_config.iteration_identifier = dag.run_id

    bibs_configuration = BibsTransformer.TaskConfiguration(
        name="bibs-transformer",
        migration_task_type="BibsTransformer",
        library_config=library_config,
        hrid_handling="default",
        files=[{ "file_name": f"{marc_stem}.mrc", "suppress": False}],
        ils_flavour="voyager",  # Voyager uses 001 field, using tag001 works
    )

    bibs_transformer = BibsTransformer(
        bibs_configuration, library_config, use_logging=False
    )

    logger.info(f"Starting bibs_tranfers work for {marc_stem}.mrc")

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





def post_folio_instance_records(**kwargs):
    """Creates new records in FOLIO"""
    dag = kwargs["dag_run"]

    batch_size = kwargs.get("MAX_ENTITIES", 1000)
    job_number = kwargs.get("job")

    with open(f"/tmp/instances-{dag.run_id}-{job_number}.json") as fo:
        instance_records = json.load(fo)

    for i in range(0, len(instance_records), batch_size):
        instance_batch = instance_records[i: i + batch_size]
        logger.info(f"Posting {len(instance_batch)} in batch {i/batch_size}")
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
    dag = kwargs["dag_run"]

    batch_size = kwargs.get("MAX_ENTITIES", 1000)
    job_number = kwargs.get("job")

    with open(f"/tmp/holdings-{dag.run_id}-{job_number}.json") as fo:
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
