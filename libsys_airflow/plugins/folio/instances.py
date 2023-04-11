import csv
import json
import logging
import pandas as pd
import requests

from datetime import datetime
from pathlib import Path

from folioclient import FolioClient
from folio_migration_tools.migration_tasks.bibs_transformer import BibsTransformer
from folio_migration_tools.library_configuration import HridHandling

from libsys_airflow.plugins.folio.helpers import post_to_okapi, setup_data_logging

logger = logging.getLogger(__name__)


def _generate_record_lookups(base_tsv: Path, lookup_stat_codes: dict) -> dict:
    """
    Generates record lookup dictionary based on values in tsv
    """
    record_lookups = {}
    logger.error(f"Base {base_tsv}")
    with base_tsv.open() as fo:
        tsv_reader = csv.DictReader(fo, delimiter="\t")
        for row in tsv_reader:
            catkey = row['CATKEY']
            discovery_suppress = False
            if int(row['CATALOG_SHADOW']) > 0:
                discovery_suppress = True
            stat_codes = []
            for item_cat in [row['ITEM_CAT1'], row['ITEM_CAT2']]:
                if item_cat in lookup_stat_codes:
                    stat_codes.append(lookup_stat_codes[item_cat])
            if catkey in record_lookups:
                record_lookups[catkey]["stat_codes"].extend(stat_codes)
                record_lookups[catkey]["suppress"] = discovery_suppress
            else:
                record_lookups[catkey] = {
                    "stat_codes": stat_codes,
                    "suppress": discovery_suppress
                }
    return record_lookups


def _remove_dup_admin_notes(record: dict):
    """
    Removes administrativeNotes that contain duplicated HRID
    """
    for i, admin_note in enumerate(record.get('administrativeNotes', [])):
        if admin_note.startswith("Identifier(s) from previous system"):
            record['administrativeNotes'].pop(i)


def _set_cataloged_date(instance: dict, dates_df: pd.DataFrame, instance_status: dict):
    ckey = instance["hrid"].removeprefix("a")
    matched_row = dates_df.loc[dates_df["CATKEY"] == ckey]
    if matched_row["CATALOGED_DATE"].values[0] != "0":
        date_cat = datetime.strptime(
            matched_row["CATALOGED_DATE"].values[0], "%Y%m%d"
        )
        instance["catalogedDate"] = date_cat.strftime("%Y-%m-%d")
        instance["statusId"] = instance_status["Cataloged"]
    else:
        instance["statusId"] = instance_status["Uncataloged"]


def _adjust_records(**kwargs):
    """
    Modifies instances records
    """
    instances_path: Path = kwargs["instances_record_path"]
    tsv_dates: str = kwargs["tsv_dates"]
    instance_status: dict = kwargs["instance_statuses"]
    base_tsv: Path = kwargs["base_tsv"]
    stat_codes: dict = kwargs["stat_codes"]

    dates_df = pd.read_csv(
        tsv_dates,
        sep="\t",
        dtype={"CATKEY": str, "CREATED_DATE": str, "CATALOGED_DATE": str},
    )

    record_lookups = _generate_record_lookups(base_tsv, stat_codes)

    records = []
    with instances_path.open() as fo:
        for row in fo.readlines():
            record = json.loads(row)
            record["_version"] = 1  # for handling optimistic locking
            stat_codes = record_lookups.get(record["hrid"], {}).get("stat_codes", [])
            if len(stat_codes) > 0:
                record["statisticalCodeIds"] = list(set(stat_codes))
            if record_lookups.get(record["hrid"], {}).get("suppress", False):
                record["discoverySuppress"] = True
            _set_cataloged_date(record, dates_df, instance_status)
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


def _get_statistical_codes(folio_client: FolioClient) -> dict:
    """
    Retrieve statistical codes for instances and saves for lookups
    """
    # Get Instance stat code type
    stat_code_type_result = requests.get(f"{folio_client.okapi_url}/statistical-code-types?query=name==Instance&limit=200",
                                         headers=folio_client.okapi_headers)
    stat_code_type_result.raise_for_status()
    instance_code_type = stat_code_type_result.json()['statisticalCodeTypes'][0]['id']
    instance_stat_codes_result = requests.get(
        f"{folio_client.okapi_url}/statistical-codes?query=statisticalCodeTypeId=={instance_code_type}&limit=200",
        headers=folio_client.okapi_headers)
    instance_stat_codes_result.raise_for_status()
    stat_code_lookup = {}
    for row in instance_stat_codes_result.json()['statisticalCodes']:
        match row['code']:

            case "E-THESIS":
                stat_code_lookup["E-THESIS"] = row["id"]

            case "LEVEL3":
                stat_code_lookup["LEVEL3-CAT"] = row["id"]
                stat_code_lookup["LEVEL3OCLC"] = row["id"]

            case "MARCIVE":
                stat_code_lookup["MARCIVE"] = row["id"]

    return stat_code_lookup


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

    base_tsv_path = (
        iteration_dir / f"source_data/items/{marc_stem}.tsv"
    )
    logger.error(f"Base tsv path {base_tsv_path} {base_tsv_path.exists()}")

    stat_codes = _get_statistical_codes(folio_client)
    logger.error(f"Stat codes {stat_codes}")

    _adjust_records(
        instances_record_path=instances_record_path,
        tsv_dates=tsv_dates,
        instance_statuses=instance_statuses,
        base_tsv=base_tsv_path,
        stat_codes=stat_codes
    )

    bibs_transformer.wrap_up()
