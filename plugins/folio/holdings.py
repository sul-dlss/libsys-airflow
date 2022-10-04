import json
import logging
import pathlib
import re

from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)
from folio_migration_tools.migration_tasks.holdings_marc_transformer import (
    HoldingsMarcTransformer,
)

from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID

from plugins.folio.helpers import post_to_okapi, setup_data_logging

logger = logging.getLogger(__name__)

vendor_code_re = re.compile(r"[a-z]+\d+")


def _run_transformer(transformer, airflow, dag_run_id):
    setup_data_logging(transformer)

    transformer.do_work()

    transformer.wrap_up()

    _add_identifiers(airflow, dag_run_id, transformer)


def _add_identifiers(
    airflow: str, dag_run_id: str, holdings_transformer: HoldingsCsvTransformer
):
    # Creates/opens file with Instance IDs
    instance_holdings_path = pathlib.Path(
        f"{airflow}/migration/iterations/{dag_run_id}/results/holdings-hrids.json"
    )

    if instance_holdings_path.exists():
        with instance_holdings_path.open() as fo:
            instance_keys = json.load(fo)
    else:
        instance_keys = {}

    holdings_path = pathlib.Path(
        f"{airflow}/migration/iterations/{dag_run_id}/results/folio_holdings_{holdings_transformer.task_configuration.name}.json"
    )

    with holdings_path.open() as fo:
        holdings_records = [json.loads(row) for row in fo.readlines()]

    logger.info("Adding HRIDs and re-generated UUIDs for holdings")

    for record in holdings_records:
        instance_uuid = record["instanceId"]
        former_id = record["formerIds"][0]
        # Adds an "h" for holdings prefix
        if former_id.startswith("a"):
            former_id = former_id[:1] + "h" + former_id[1:]
        if instance_uuid in instance_keys:
            new_count = instance_keys[instance_uuid] + 1
        else:
            new_count = 1
        instance_keys[instance_uuid] = new_count
        record["hrid"] = f"{former_id}_{new_count}"

        # Adds Determinstic UUID based on CATKEY and HRID
        record["id"] = str(
            FolioUUID(
                holdings_transformer.folio_client.okapi_url,
                FOLIONamespaces.holdings,
                f"{record['formerIds'][0]}{record['hrid']}",
            )
        )

        # To handle optimistic locking
        record["_version"] = 1

    with instance_holdings_path.open("w+") as fo:
        json.dump(instance_keys, fo)

    with holdings_path.open("w+") as fo:
        for holding in holdings_records:
            fo.write(f"{json.dumps(holding)}\n")

    # Saves holdings id maps to backup
    with open(
        f"{airflow}/migration/iterations/{dag_run_id}/results/holdings_id_map_all.json",
        "a+",
    ) as all_id_map:
        for holding in holdings_records:
            lookup = {"legacy_id": holding["formerIds"][0], "folio_id": holding["id"]}
            all_id_map.write(f"{json.dumps(lookup)}\n")


def post_folio_holding_records(**kwargs):
    """Creates/overlays Holdings records in FOLIO"""
    dag = kwargs["dag_run"]

    tmp_location = kwargs.get("tmp_dir", "/tmp")

    batch_size = int(kwargs.get("MAX_ENTITIES", 1000))
    job_number = kwargs.get("job")

    with open(f"{tmp_location}/holdings-{dag.run_id}-{job_number}.json") as fo:
        holding_records = json.load(fo)

    for i in range(0, len(holding_records), batch_size):
        holdings_batch = holding_records[i : i + batch_size]  # noqa
        logger.info(f"Posting {i} to {i+batch_size} holding records")
        post_to_okapi(
            token=kwargs["task_instance"].xcom_pull(
                key="return_value", task_ids="post-to-folio.folio_login"
            ),
            records=holdings_batch,
            endpoint="/holdings-storage/batch/synchronous?upsert=true",
            payload_key="holdingsRecords",
            **kwargs,
        )


def run_holdings_tranformer(*args, **kwargs):
    dag = kwargs["dag_run"]
    library_config = kwargs["library_config"]
    airflow = kwargs.get("airflow", "/opt/airflow")

    library_config.iteration_identifier = dag.run_id

    holdings_stem = kwargs["holdings_stem"]

    holdings_configuration = HoldingsCsvTransformer.TaskConfiguration(
        name="holdings-transformer",
        migration_task_type="HoldingsCsvTransformer",
        hrid_handling="preserve001",
        files=[{"file_name": f"{holdings_stem}.tsv", "suppress": False}],
        create_source_records=False,
        call_number_type_map_file_name="call_number_type_mapping.tsv",
        holdings_map_file_name="holdingsrecord_mapping.json",
        location_map_file_name="locations.tsv",
        holdings_type_uuid_for_boundwiths="",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    )

    holdings_transformer = HoldingsCsvTransformer(
        holdings_configuration, library_config, use_logging=False
    )

    _run_transformer(holdings_transformer, airflow, dag.run_id)

    logger.info(f"Finished transforming {holdings_stem}.tsv to FOLIO holdings")


def run_mhld_holdings_transformer(*args, **kwargs):
    dag = kwargs["dag_run"]
    library_config = kwargs["library_config"]
    task_instance = kwargs["task_instance"]
    airflow = kwargs.get("airflow", "/opt/airflow")

    mhld_file = task_instance.xcom_pull(task_ids="bib-files-group", key="mhld-file")

    if not mhld_file or len(mhld_file) < 1:
        logger.info("No MHLD files found, exiting task")
        return

    library_config.iteration_identifier = dag.run_id

    filepath = pathlib.Path(mhld_file)

    mhld_holdings_config = HoldingsMarcTransformer.TaskConfiguration(
        name="holdings-mhld-transformer",
        migration_task_type="HoldingsMarcTransformer",
        legacy_id_marc_path="001",
        use_tenant_mapping_rules=False,
        hrid_handling="default",
        files=[{"file_name": filepath.name, "supressed": False}],
        mfhd_mapping_file_name="mhld_rules.json",
        location_map_file_name="locations-mhld.json",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed",
        create_source_records=True,
    )

    holdings_transformer = HoldingsMarcTransformer(
        mhld_holdings_config, library_config, use_logging=False
    )

    _run_transformer(holdings_transformer, airflow, dag.run_id)

    logger.info(f"Finished transforming MHLD {filepath.name} to FOLIO holdings")


def electronic_holdings(*args, **kwargs) -> str:
    """Generates FOLIO Holdings records from Symphony 856 fields"""
    dag = kwargs["dag_run"]
    holdings_stem = kwargs["holdings_stem"]
    library_config = kwargs["library_config"]
    holdings_type_id = kwargs["electronic_holdings_id"]
    airflow = kwargs.get("airflow", "/opt/airflow")

    filename = f"{holdings_stem}.electronic.tsv"
    full_path = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/source_data/items/{filename}"
    )

    if not full_path.exists():
        logger.info(f"Electronic Holdings {full_path} does not exist")
        return

    library_config.iteration_identifier = dag.run_id

    holdings_configuration = HoldingsCsvTransformer.TaskConfiguration(
        name="holdings-electronic-transformer",
        migration_task_type="HoldingsCsvTransformer",
        hrid_handling="preserve001",
        files=[{"file_name": filename, "suppress": False}],
        create_source_records=False,
        call_number_type_map_file_name="call_number_type_mapping.tsv",
        holdings_map_file_name="holdingsrecord_mapping_electronic.json",
        location_map_file_name="locations.tsv",
        holdings_type_uuid_for_boundwiths="",
        holdings_merge_criteria=["instanceId", "permanentLocationId"],
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id=holdings_type_id,
    )

    holdings_transformer = HoldingsCsvTransformer(
        holdings_configuration, library_config, use_logging=False
    )

    _run_transformer(holdings_transformer, airflow, dag.run_id)

    logger.info(f"Finished transforming electronic {filename} to FOLIO holdings")


def consolidate_holdings_map(*args, **kwargs):
    dag = kwargs["dag_run"]
    airflow = kwargs.get("airflow", "/opt/airflow")
    last_id_map = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/results/holdings_id_map_{dag.run_id}.json"
    )
    all_id_map = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/results/holdings_id_map_all.json"
    )
    all_id_map.rename(last_id_map)
    logger.info(f"Finished moving {all_id_map} to {last_id_map}")


def update_mhlds_uuids(*args, **kwargs):
    """Updates Holdings UUID in MHLDs SRS file"""
    dag = kwargs["dag_run"]
    airflow = kwargs.get("airflow", "/opt/airflow")

    mhld_srs_path = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/results/folio_srs_holdings_holdings-mhld-transformer.json"
    )

    if not mhld_srs_path.exists():
        logger.info("No MHLD SRS records")
        return

    holdings_id_map_path = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/results/holdings_id_map_{dag.run_id}.json"
    )

    holdings_id_map = {}
    with holdings_id_map_path.open() as fo:
        for line in fo.readlines():
            holdings_rec = json.loads(line)
            holdings_id_map[holdings_rec["legacy_id"]] = holdings_rec["folio_id"]

    updated_srs_records = []
    with mhld_srs_path.open() as fo:
        for line in fo.readlines():
            mhld_srs_record = json.loads(line)
            hrid = mhld_srs_record["externalIdsHolder"]["holdingsHrid"]
            if hrid in holdings_id_map:
                mhld_srs_record["externalIdsHolder"]["holdingsId"] = holdings_id_map[
                    hrid
                ]
                updated_srs_records.append(mhld_srs_record)
            else:
                logger.error(f"UUID for MHLD {hrid} not found in SRS record")

    with mhld_srs_path.open("w+") as fo:
        for record in updated_srs_records:
            fo.write(f"{json.dumps(record)}\n")

    logger.info(
        f"Finished updated Holdings UUID for {len(updated_srs_records):,} MHLD SRS records"
    )
