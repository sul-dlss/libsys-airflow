import csv
import json
import logging
import pathlib
import re

from functools import partialmethod

from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID
from folioclient import FolioClient

from airflow.models import Variable

from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)
from folio_migration_tools.migration_tasks.holdings_marc_transformer import (
    HoldingsMarcTransformer,
)

from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)

from plugins.folio.helpers import post_to_okapi, setup_data_logging, constants
from plugins.folio.helpers.marc import filter_mhlds

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


def _alt_get_legacy_ids(*args):
    """
    This function overrides RulesMapperHolding method for MHLDs so
    that duplicate CATKEYs in the 004 will generate separate records
    """
    marc_record = args[2]
    field_001 = marc_record["001"].value()
    library, location = "", ""
    if "852" in marc_record:
        library = marc_record["852"]["b"]
        location = marc_record["852"]["c"]
    return [f"{field_001} {library} {location}"]


def _ignore_coded_holdings_statements(*args):
    """
    This function overrides RulesMapperHolding method for mapping
    various 85x and 86x fields to HoldingsStatements, we want to just
    use the MARC Holdings map from the FOLIO server
    """
    pass


def _process_mhld(**kwargs) -> dict:
    """
    Takes a mhld record and it's corresponding SRS record, updates HRID and
    SRS record and returns the updated SRS record
    """
    mhld_record: dict = kwargs["mhld_record"]
    srs_record: dict = kwargs["srs_record"]
    all_holdings: dict = kwargs["all_holdings"]
    instance_map: dict = kwargs["instance_map"]
    locations_lookup: dict = kwargs["locations_lookup"]

    instance_id = mhld_record["instanceId"]
    instance = instance_map[instance_id]

    current_count = len(instance["holdings"])
    instance_hrid = instance["hrid"]
    holding_id = mhld_record["id"]
    holdings_hrid = f"{instance_hrid[:1]}h{instance_hrid[1:]}_{current_count + 1}"
    mhld_record["hrid"] = holdings_hrid
    all_holdings[holding_id] = mhld_record
    mhld_record["_version"] = 1
    instance["holdings"][holding_id] = {
        "permanentLocationId": mhld_record["permanentLocationId"]
    }
    srs_record = _update_srs_ids(all_holdings[holding_id], srs_record, locations_lookup)
    return srs_record


def _update_srs_ids(
    mhld_record: dict, srs_record: dict, locations_lookup: dict
) -> dict:
    existing_holdings_uuid = mhld_record["id"]
    existing_hrid = mhld_record["hrid"]
    location_id = mhld_record["permanentLocationId"]
    srs_record["externalIdsHolder"]["holdingsHrid"] = existing_hrid
    srs_record["externalIdsHolder"]["holdingsId"] = existing_holdings_uuid
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
                            row[subfield] = locations_lookup[location_id]

                        case "c":
                            field[tag]["subfields"].pop(i)

            case "999":
                for row in field[tag]["subfields"]:
                    subfield = list(row)[0]
                    match subfield:
                        case "i":
                            row[subfield] = mhld_record["id"]

                        case "s":
                            row["s"] = srs_record["id"]

    srs_record["rawRecord"]["id"] = srs_record["id"]
    srs_record["rawRecord"]["content"] = json.dumps(
        srs_record["parsedRecord"]["content"]
    )
    return srs_record


def update_holdings(**kwargs):

    folio_client = kwargs.get("folio_client")

    if folio_client is None:
        folio_client = FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )

    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]
    iteration_dir = pathlib.Path(f"{airflow}/migration/iterations/{dag.run_id}")
    holdings_path = iteration_dir / "results/folio_holdings.json"
    mhld_holdings_path = iteration_dir / "results/folio_holdings_mhld-transformer.json"

    if not mhld_holdings_path.exists():
        logger.info(f"No MHLDs holdings {mhld_holdings_path}, exiting")
        return

    srs_path = iteration_dir / "results/folio_srs_holdings_mhld-transformer.json"

    with mhld_holdings_path.open() as fo:
        mhld_holdings = [json.loads(line) for line in fo.readlines()]

    with srs_path.open() as fo:
        srs_records = [json.loads(line) for line in fo.readlines()]

    with (iteration_dir / "results/instance-holdings-items.json").open() as fo:
        instance_map = json.load(fo)

    locations_lookup = {}
    for location in folio_client.locations:
        locations_lookup[location["id"]] = location["code"]

    all_holdings = _generate_lookups(holdings_path)

    updated_srs_records = []
    count = 0
    for mhld, srs in zip(mhld_holdings, srs_records):
        updated_srs = _process_mhld(
            mhld_record=mhld,
            srs_record=srs,
            all_holdings=all_holdings,
            instance_map=instance_map,
            locations_lookup=locations_lookup,
        )
        updated_srs_records.append(updated_srs)
        if not count % 1_000:
            logger.info(f"Updated {count:,} MHLD and SRS records")
        count += 1

    with srs_path.open("w+") as fo:
        for record in updated_srs_records:
            fo.write(f"{json.dumps(record)}\n")

    with (iteration_dir / "results/folio_holdings.json").open("w+") as fo:
        for holdings_record in all_holdings.values():
            if "formerIds" in holdings_record:
                del (holdings_record["formerIds"])
            for i, admin_note in enumerate(holdings_record.get("administrativeNotes", [])):
                if admin_note.startswith("Identifier(s) from previous system"):
                    holdings_record["administrativeNotes"].pop(i)
            fo.write(f"{json.dumps(holdings_record)}\n")

    mhld_holdings_path.unlink()
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

    filter_mhlds(
        pathlib.Path(f"{airflow}/migration/iterations/{dag.run_id}/source_data/holdings/{filepath.name}")
    )

    mhld_holdings_config = HoldingsMarcTransformer.TaskConfiguration(
        name="mhld-transformer",
        migration_task_type="HoldingsMarcTransformer",
        legacy_id_marc_path="004",
        hrid_handling="default",
        files=[{"file_name": filepath.name, "supressed": False}],
        mfhd_mapping_file_name="mhld_rules.json",
        location_map_file_name="locations-mhld.tsv",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed",
        create_source_records=True,
        never_update_hrid_settings=True,
    )

    # Overrides static method to allow duplicate CATKEYs in MHLD records
    RulesMapperHoldings.get_legacy_ids = partialmethod(
        _alt_get_legacy_ids, RulesMapperHoldings
    )

    # Overrides method that applies default mappings for 85x and 86x fields
    RulesMapperHoldings.parse_coded_holdings_statements = partialmethod(
        _ignore_coded_holdings_statements, RulesMapperHoldings
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


def boundwith_holdings(*args, **kwargs):
    okapi_url = Variable.get("OKAPI_URL")
    folio_client = kwargs.get("folio_client")

    dag = kwargs["dag_run"]
    task_instance = kwargs["task_instance"]
    airflow = kwargs.get("airflow", "/opt/airflow")

    iteration_dir = pathlib.Path(f"{airflow}/migration/iterations/{dag.run_id}")
    bwchild_file = task_instance.xcom_pull(task_ids="bib-files-group", key="bwchild-file")
    if bwchild_file is None:
        logger.error("Boundwidth child file does not exist, exiting")
        return
    bw_tsv_path = pathlib.Path(bwchild_file)

    bw_holdings_json_path = iteration_dir / "results/folio_holdings_boundwith.json"
    bw_parts_json_path = iteration_dir / "results/boundwith_parts.json"

    if folio_client is None:
        folio_client = FolioClient(
            okapi_url, "sul", Variable.get("FOLIO_USER"), Variable.get("FOLIO_PASSWORD")
        )

    locations_lookup = {}
    for location in folio_client.locations:
        if "SEE-OTHER" in location["code"]:
            locations_lookup[location["code"]] = location["id"]

    with bw_tsv_path.open(encoding="utf-8-sig") as tsv:
        logger.info("Processing boundwiths")
        bw_reader = csv.DictReader(tsv, delimiter="\t")

        with bw_holdings_json_path.open("w+") as bwh, bw_parts_json_path.open(
            "w+"
        ) as bwp:
            for row in bw_reader:
                """
                includes default holdings-type id for 'Bound-with'
                """
                holdings_id = str(
                    FolioUUID(
                        okapi_url,
                        FOLIONamespaces.holdings,
                        f"{row['CATKEY']}{row['CALL_SEQ']}{row['COPY']}",
                    )
                )
                loc_code = f"{constants.see_other_lib_locs.get(row.get('LIBRARY'))}"

                perm_loc_id = locations_lookup.get(loc_code)
                if perm_loc_id is None:
                    logger.error(f"Failed to find location for {row}")
                    continue

                holdings = {
                    "id": holdings_id,
                    "instanceId": str(
                        FolioUUID(
                            okapi_url, FOLIONamespaces.instances, f"a{row['CATKEY']}"
                        )
                    ),
                    "callNumber": row["BASE_CALL_NUMBER"],
                    "callNumberTypeId": constants.call_number_codes[
                        row["CALL_NUMBER_TYPE"]
                    ],
                    "permanentLocationId": perm_loc_id,
                    "holdingsTypeId": "5b08b35d-aaa3-4806-998c-9cd85e5bc406",
                }

                logger.info(f"Writing holdings id {holdings_id} to file")
                bwh.write(f"{json.dumps(holdings)}\n")

                bw_id = str(
                    FolioUUID(
                        okapi_url,
                        FOLIONamespaces.other,
                        row["CATKEY"] + row["BARCODE"],
                    )
                )

                bw_parts = {
                    "id": bw_id,
                    "holdingsRecordId": holdings_id,
                    "itemId": str(
                        FolioUUID(okapi_url, FOLIONamespaces.items, row["BARCODE"])
                    ),
                }

                logger.info(f"Writing bound-with part id {bw_id} to file")
                bwp.write(f"{json.dumps(bw_parts)}\n")

    bw_tsv_path.unlink()
