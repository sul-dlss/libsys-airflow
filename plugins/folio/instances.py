import json
import logging
import pandas as pd
import requests

from datetime import datetime
from pathlib import Path

from folioclient import FolioClient
from folio_migration_tools.migration_tasks.bibs_transformer import BibsTransformer
from folio_migration_tools.library_configuration import HridHandling

from plugins.folio.helpers import post_to_okapi, setup_data_logging

logger = logging.getLogger(__name__)


def _remove_dup_admin_notes(record: dict):
    """
    Removes administrativeNotes that contain duplicated HRID
    """
    for i, admin_note in enumerate(record.get('administrativeNotes', [])):
        if admin_note.startswith("Identifier(s) from previous system"):
            record['administrativeNotes'].pop(i)


def _adjust_records(instances_path: Path, tsv_dates: str, instance_status: dict):

    dates_df = pd.read_csv(
        tsv_dates,
        sep="\t",
        dtype={"CATKEY": str, "CREATED_DATE": str, "CATALOGED_DATE": str},
    )
    records = []
    with instances_path.open() as fo:
        for row in fo.readlines():
            record = json.loads(row)
            record["_version"] = 1  # for handling optimistic locking
            ckey = record["hrid"].removeprefix("a")
            matched_row = dates_df.loc[dates_df["CATKEY"] == ckey]
            if matched_row["CATALOGED_DATE"].values[0] != "0":
                date_cat = datetime.strptime(
                    matched_row["CATALOGED_DATE"].values[0], "%Y%m%d"
                )
                record["catalogedDate"] = date_cat.strftime("%Y-%m-%d")
                record["statusId"] = instance_status["Cataloged"]
            else:
                record["statusId"] = instance_status["Uncataloged"]
            _remove_dup_admin_notes(record)
            records.append(record)
    with instances_path.open("w+") as fo:
        for record in records:
            fo.write(f"{json.dumps(record)}\n")
    Path(tsv_dates).unlink()


def _get_instance_status(folio_client: FolioClient) -> dict:
    """
    Retrieves instance statuses, iterates through list, retrieves name and
    id, and returns a lookup dict of Name-Identifiers
    """
    instance_statuses = {}
    instance_statuses_result = requests.get(
        f"{folio_client.okapi_url}/instance-statuses?limit=1000",
        headers=folio_client.okapi_headers,
    )
    for row in instance_statuses_result.json()["instanceStatuses"]:
        instance_statuses[row["name"]] = row["id"]
    return instance_statuses


def post_folio_instance_records(**kwargs):
    """Creates new records in FOLIO"""
    dag = kwargs["dag_run"]

    batch_size = int(kwargs.get("MAX_ENTITIES", 1000))
    job_number = kwargs.get("job")

    with open(f"/tmp/instances-{dag.run_id}-{job_number}.json") as fo:
        instance_records = json.load(fo)

    for i in range(0, len(instance_records), batch_size):
        instance_batch = instance_records[i:i + batch_size]
        logger.info(f"Posting {len(instance_batch)} in batch {i/batch_size}")
        post_to_okapi(
            token=kwargs["task_instance"].xcom_pull(
                key="return_value", task_ids="post-to-folio.folio_login"
            ),
            records=instance_batch,
            endpoint="/instance-storage/batch/synchronous?upsert=true",
            payload_key="instances",
            **kwargs,
        )


def run_bibs_transformer(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")

    dag = kwargs["dag_run"]

    iteration_dir = Path(f"{airflow}/migration/iterations/{dag.run_id}")

    library_config = kwargs["library_config"]

    marc_stem = kwargs["marc_stem"]

    tsv_dates = kwargs["dates_tsv"]

    folio_client = kwargs.get("folio_client")
    if folio_client is None:
        folio_client = FolioClient(
            library_config.okapi_url,
            library_config.tenant_id,
            library_config.okapi_username,
            library_config.okapi_password,
        )

    library_config.iteration_identifier = dag.run_id

    bibs_configuration = BibsTransformer.TaskConfiguration(
        name="bibs-transformer",
        migration_task_type="BibsTransformer",
        library_config=library_config,
        hrid_handling=HridHandling.preserve001,
        never_update_hrid_settings=True,
        files=[{"file_name": f"{marc_stem}.mrc", "suppress": False}],
        ils_flavour="tag001",
    )

    bibs_transformer = BibsTransformer(
        bibs_configuration, library_config, use_logging=False
    )

    setup_data_logging(bibs_transformer)

    logger.info(f"Starting bibs_tranfers work for {marc_stem}.mrc")

    bibs_transformer.do_work()

    instances_record_path = (
        iteration_dir / "results/folio_instances_bibs-transformer.json"
    )

    instance_statuses = _get_instance_status(folio_client)

    _adjust_records(instances_record_path, tsv_dates, instance_statuses)

    bibs_transformer.wrap_up()
