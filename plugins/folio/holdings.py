import json
import logging
import pathlib
import re

from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID
from airflow.models import Variable

from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)
from folio_migration_tools.migration_tasks.holdings_marc_transformer import (
    HoldingsMarcTransformer,
)

from plugins.folio.helpers import post_to_okapi, setup_data_logging

logger = logging.getLogger(__name__)

vendor_code_re = re.compile(r"[a-z]+\d+")


def _run_transformer(transformer, airflow, dag_run_id, item_path):
    setup_data_logging(transformer)

    transformer.do_work()

    transformer.wrap_up()


def _generate_lookups(holdings_tsv_path: pathlib.Path) -> tuple:
    """
    Takes FOLIO holdings file, creates a dictionary by holdings id for
    merging MHLD records with existing holdings records
    """
    all_holdings = dict()
    with holdings_tsv_path.open() as fo:
        for line in fo.readlines():
            holding = json.loads(line)
            holding_id = holding["id"]
            all_holdings[holding_id] = holding
    return all_holdings


def _mhld_into_holding(mhld_record: dict, holding_record: dict) -> dict:
    for name in [
        "holdingsStatements",
        "holdingsStatementsForIndexes",
        "holdingsStatementsForSupplements",
        "notes",
    ]:
        if name in mhld_record:
            if name in holding_record:
                holding_record[name].extend(mhld_record[name])
            else:
                holding_record[name] = mhld_record[name]
    # Change Source to MARC to display MHLD MARC record in FOLIO
    holding_record['sourceId'] = mhld_record['sourceId']
    return holding_record


def _process_mhld(**kwargs) -> dict:
    """
    Takes a mhld record and it's corresponding SRS record, attempts to
    merge into existing holdings record, and then updates and returns
    the updated SRS record
    """

    mhld_record: dict = kwargs["mhld_record"]
    srs_record: dict = kwargs["srs_record"]
    all_holdings: dict = kwargs["all_holdings"]
    instance_map: dict = kwargs["instance_map"]
    okapi_url: str = kwargs["okapi_url"]
    iteration_dir: pathlib.Path = kwargs["iteration_dir"]

    instance_id = mhld_record["instanceId"]
    instance = instance_map[instance_id]
    merged_holding = None

    mhld_report = iteration_dir / "reports/report_mhld-merges.md"
    mhld_report.write_text("# MHLD Merge Report\n")

    for holding_id, info in instance["holdings"].items():
        location_id = info["location_id"]
        if (
            location_id == mhld_record["permanentLocationId"]
            and info["merged"] is False
        ):
            holdings_rec = all_holdings[holding_id]
            merged_holding = _mhld_into_holding(mhld_record, holdings_rec)
            all_holdings[holding_id] = merged_holding
            info["merged"] = True
            with mhld_report.open("a") as fo:
                fo.write(f"Merged {mhld_record['id']} into {holdings_rec['id']}\n\n")
            break

    # Use MHLD record if failed to merge into existing holding
    if merged_holding is None:
        current_count = len(instance["holdings"])
        instance_hrid = instance["hrid"]
        holdings_hrid = f"{instance_hrid[:1]}h{instance_hrid[1:]}_{current_count + 1}"
        mhld_record["hrid"] = holdings_hrid
        holding_id = str(FolioUUID(okapi_url, FOLIONamespaces.holdings, holdings_hrid))
        mhld_record["id"] = holding_id
        all_holdings[holding_id] = mhld_record
        mhld_record["_version"] = 1
        instance["holdings"][holding_id] = {
            "location_id": mhld_record["permanentLocationId"],
            "merged": True,
        }
        with mhld_report.open("a") as fo:
            fo.write(
                f"\nNo match found in existing Holdings record for {holding_id}\n\n"
            )
    srs_record = _update_srs_ids(all_holdings[holding_id], srs_record, okapi_url)
    return srs_record


def _update_srs_ids(mhld_record: dict, srs_record: dict, okapi_url: str) -> dict:

    existing_hrid = mhld_record["hrid"]
    existing_holdings_uuid = mhld_record["id"]
    location_id = mhld_record["permanentLocationId"]
    new_srs_record_id = str(
        FolioUUID(
            okapi_url,
            FOLIONamespaces.srs_records_holdingsrecord,
            existing_hrid,
        )
    )
    srs_record["id"] = new_srs_record_id
    srs_record["matchedId"] = new_srs_record_id
    srs_record["externalIdsHolder"]["holdingsId"] = existing_holdings_uuid
    srs_record["externalIdsHolder"]["holdingsHrid"] = existing_hrid
    srs_record["parsedRecord"]["id"] = new_srs_record_id
    for field in srs_record["parsedRecord"]["content"]["fields"]:
        tag = list(field.keys())[0]
        match tag:
            case "001":
                field[tag] = existing_hrid

            case "852":
                for i, row in enumerate(field[tag]["subfields"]):
                    subfield = list(row)[0]
                    match subfield:
                        case "b":
                            row[subfield] = location_id

                        case "c":
                            field[tag]["subfields"].pop(i)

            case "999":
                for row in field[tag]["subfields"]:
                    subfield = list(row)[0]
                    match subfield:
                        case "i":
                            row[subfield] = existing_holdings_uuid

                        case "s":
                            row["s"] = new_srs_record_id

    srs_record["rawRecord"]["id"] = new_srs_record_id
    srs_record["rawRecord"]["content"] = json.dumps(
        srs_record["parsedRecord"]["content"]
    )
    return srs_record


def merge_update_holdings(**kwargs):
    okapi_url = Variable.get("OKAPI_URL")
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]
    iteration_dir = pathlib.Path(f"{airflow}/migration/iterations/{dag.run_id}")
    holdings_tsv_path = iteration_dir / "results/folio_holdings_tsv-transformer.json"
    mhld_holdings_path = iteration_dir / "results/folio_holdings_mhld-transformer.json"

    if not mhld_holdings_path.exists():
        logger.info(f"No MHLDs holdings {mhld_holdings_path}, exiting")
        return

    srs_path = iteration_dir / "results/folio_srs_holdings_mhld-transformer.json"

    with mhld_holdings_path.open() as fo:
        mhld_holdings = [json.loads(line) for line in fo.readlines()]

    with srs_path.open() as fo:
        srs_records = [json.loads(line) for line in fo.readlines()]

    with (iteration_dir / "results/instance_holdings_map.json").open() as fo:
        instance_map = json.load(fo)

    all_holdings= _generate_lookups(holdings_tsv_path)

    updated_srs_records = []
    count = 0
    for mhld, srs in zip(mhld_holdings, srs_records):
        updated_srs = _process_mhld(
            mhld_record=mhld,
            srs_record=srs,
            all_holdings=all_holdings,
            instance_map=instance_map,
            okapi_url=okapi_url,
            iteration_dir=iteration_dir,
        )
        updated_srs_records.append(updated_srs)
        if not count % 1_000:
            logger.info(f"Merged and updated {count:,} MHLD and SRS records")
        count += 1

    with srs_path.open("w+") as fo:
        for record in updated_srs_records:
            fo.write(f"{json.dumps(record)}\n")

    with (iteration_dir / "results/folio_holdings.json").open("w+") as fo:
        for holdings_record in all_holdings.values():
            fo.write(f"{json.dumps(holdings_record)}\n")

    # Remove existing holdings JSON files
    mhld_holdings_path.unlink()
    holdings_tsv_path.unlink()

    logger.info("Finished merging MHLDS holdings records and updating SRS records")


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

    holdings_filepath = (
        library_config.base_folder
        / f"iterations/{dag.run_id}/source_data/items/{holdings_stem}.tsv"
    )

    holdings_configuration = HoldingsCsvTransformer.TaskConfiguration(
        name="tsv-transformer",
        migration_task_type="HoldingsCsvTransformer",
        hrid_handling="default",
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
        hrid_handling="default",
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
