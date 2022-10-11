import json
import logging
import pathlib
import re


from airflow.models import Variable

from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)
from folio_migration_tools.migration_tasks.holdings_marc_transformer import (
    HoldingsMarcTransformer,
)

from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID

from plugins.folio.helpers import post_to_okapi, setup_data_logging
from plugins.folio.helpers.tsv import update_items

logger = logging.getLogger(__name__)

vendor_code_re = re.compile(r"[a-z]+\d+")


def _run_transformer(transformer, airflow, dag_run_id, item_path):
    setup_data_logging(transformer)

    transformer.do_work()

    transformer.wrap_up()

    _add_identifiers(airflow, dag_run_id, transformer, item_path)


def _add_identifiers(
    airflow: str,
    dag_run_id: str,
    holdings_transformer: HoldingsCsvTransformer,
    item_path: pathlib.Path
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
            lookup = {"legacy_id": holding["hrid"], "folio_id": holding["id"]}
            all_id_map.write(f"{json.dumps(lookup)}\n")

    # Updates items tsv replacing CATKEY values with new holdings HRIDs
    if item_path: 
        update_items(
            item_path,
            holdings_records,
            holdings_transformer.mapper.location_mapping.regular_mappings)


def _extract_catkey(mhld_record: list) -> str:
    for field in mhld_record["parsedRecord"]["content"]["fields"]:
        if "004" in field.keys():
            return field["004"]


def _update_mhld_ids(mhld_record: dict, existing_ids: dict) -> dict:
    okapi_url = Variable.get("OKAPI_URL")
    existing_hrid = existing_ids["hrid"]
    existing_holdings_uuid = existing_ids["id"]
    new_mhld_record_id = str(
        FolioUUID(
            okapi_url,
            FOLIONamespaces.srs_records_holdingsrecord,
            existing_hrid,
        )
    )
    mhld_record["id"] = new_mhld_record_id
    mhld_record["matchedId"] = new_mhld_record_id
    mhld_record["externalIdsHolder"]["holdingsId"] = existing_holdings_uuid
    mhld_record["externalIdsHolder"]["holdingsHrid"] = existing_hrid
    mhld_record["parsedRecord"]["id"] = new_mhld_record_id
    for field in mhld_record["parsedRecord"]["content"]["fields"]:
        if "001" in field.keys():
            field["001"] = existing_hrid
        if "999" in field.keys():
            for subfield in field["999"]["subfields"]:
                if "i" in subfield.keys():
                    subfield["i"] = existing_holdings_uuid
                if "s" in subfield.keys():
                    subfield["s"] = new_mhld_record_id
    mhld_record["rawRecord"]["id"] = new_mhld_record_id
    mhld_record["rawRecord"]["content"] = json.dumps(
        mhld_record["parsedRecord"]["content"]
    )
    return mhld_record


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

    holdings_filepath = library_config.base_folder / f"iterations/{dag.run_id}/source_data/items/{holdings_stem}.tsv"

    holdings_configuration = HoldingsCsvTransformer.TaskConfiguration(
        name="csv-transformer",
        migration_task_type="HoldingsCsvTransformer",
        hrid_handling="preserve001",
        files=[{"file_name": holdings_filepath.name, "suppress": False}],
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

    _run_transformer(holdings_transformer, airflow, dag.run_id, holdings_filepath)

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
        name="mhld-transformer",
        migration_task_type="HoldingsMarcTransformer",
        legacy_id_marc_path="001",
        use_tenant_mapping_rules=False,
        hrid_handling="default",
        files=[{"file_name": filepath.name, "supressed": False}],
        mfhd_mapping_file_name="mhld_rules.json",
        location_map_file_name="locations-mhld.tsv",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed",
        create_source_records=True,
        never_update_hrid_settings=True,
    )

    holdings_transformer = HoldingsMarcTransformer(
        mhld_holdings_config, library_config, use_logging=False
    )

    _run_transformer(holdings_transformer, airflow, dag.run_id, None)

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
        name="electronic-transformer",
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

    _run_transformer(holdings_transformer, airflow, dag.run_id, full_path)

    logger.info(f"Finished transforming electronic {filename} to FOLIO holdings")


def consolidate_holdings_map(*args, **kwargs):
    dag = kwargs["dag_run"]
    airflow = kwargs.get("airflow", "/opt/airflow")
    last_id_map = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/results/holdings_id_map.json"
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
        f"{airflow}/migration/iterations/{dag.run_id}/results/folio_srs_holdings_mhld-transformer.json"
    )

    if not mhld_srs_path.exists():
        logger.info("No MHLD SRS records")
        return

    holdings_records_path = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/results/folio_holdings_mhld-transformer.json"
    )

    holdings_map = {}
    with holdings_records_path.open() as fo:
        for line in fo.readlines():
            holdings_rec = json.loads(line)
            catkey = holdings_rec["formerIds"][0]
            body = {"id": holdings_rec["id"], "hrid": holdings_rec["hrid"]}
            if catkey in holdings_map:
                holdings_map[catkey].append(body)
            else:
                holdings_map[catkey] = [
                    body,
                ]

    updated_srs_records = []
    catkey_count = {}
    with mhld_srs_path.open() as fo:
        for line in fo.readlines():
            mhld_srs_record = json.loads(line)
            catkey = _extract_catkey(mhld_srs_record)
            if catkey in catkey_count:
                count = catkey_count[catkey] + 1
            else:
                count = 0
            catkey_count[catkey] = count
            if catkey is None:
                logger.error(f"No catkey from 004 field found in SRS record {mhld_srs_record['id']}")
                continue
            if catkey in holdings_map:
                mhld_srs_record = _update_mhld_ids(
                    mhld_srs_record, holdings_map[catkey][count]
                )
                updated_srs_records.append(mhld_srs_record)
            else:
                logger.error(f"UUID for MHLD {catkey} not found in SRS record")

    with mhld_srs_path.open("w+") as fo:
        for record in updated_srs_records:
            fo.write(f"{json.dumps(record)}\n")

    logger.info(
        f"Finished updated Holdings UUID for {len(updated_srs_records):,} MHLD SRS records"
    )
