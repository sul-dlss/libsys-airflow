import csv
import json
import logging
import pathlib
import re

import pandas as pd

from functools import partialmethod
from typing import Tuple

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

from folio_migration_tools.marc_rules_transformation.conditions import Conditions

from libsys_airflow.plugins.folio.helpers import (
    post_to_okapi,
    setup_data_logging,
    constants,
)
from libsys_airflow.plugins.folio.helpers.marc import filter_mhlds

logger = logging.getLogger(__name__)

vendor_code_re = re.compile(r"[a-z]+\d+")


def _run_transformer(transformer, airflow, dag_run_id, item_path):
    setup_data_logging(transformer)

    transformer.do_work()

    transformer.wrap_up()


def _generate_lookups(holdings_tsv_path: pathlib.Path) -> dict:
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


def _add_holdings_type_ids(**kwargs):
    """
    Looks-up and assigns Holdings Id based on FORMAT value in tsv
    """
    airflow = kwargs.get("airflow")
    dag_run_id = kwargs.get("dag_run_id")
    folio_client = kwargs.get("folio_client")
    holdings_type_result = folio_client.folio_get("/holdings-types?limit=200")
    holdings_type_lookup = {}
    for row in holdings_type_result["holdingsTypes"]:
        holdings_type_lookup[row["name"]] = row["id"]

    holdings_file = pathlib.Path(
        f"{airflow}/migration/iterations/{dag_run_id}/results/folio_holdings_tsv-transformer.json"
    )
    holdings = []
    with holdings_file.open() as fo:
        for line in fo.readlines():
            record = json.loads(line)
            record["holdingsTypeId"] = holdings_type_lookup.get(
                constants.symphony_holdings_types_map.get(
                    record["holdingsTypeId"], "Unknown"
                )
            )
            holdings.append(record)

    with holdings_file.open("w+") as fo:
        for record in holdings:
            fo.write(f"{json.dumps(record)}\n")

    logger.info(f"Finished updating {len(holdings):,} holdingsTypeIds")


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


def _alt_condition_remove_ending_punc(*args):
    """
    This function overrides the condition_remove_ending_punc method to use
    the same ending characters as is used by FOLIO data-import-processing-core
    """
    v = args[2]
    chars = ";:,/+= "
    while any(v) > 0 and v[-1] in chars:
        v = v.rstrip(v[-1])
    return v


def _wrap_additional_mapping(func):
    """
    Decorator that moves the top-level properties from the holdingsStatements
    in the Holdings Record.
    """
    top_level_props = {
        "callNumberTypeId",
        "permanentLocationId",
        "callNumber",
        "callNumberPrefix",
        "shelvingTitle",
        "callNumberSuffix",
        "copyNumber",
    }

    def _filter_property(holdings, holding_property):
        filtered_holdings = []
        for row in holdings.get(holding_property, []):
            existing_props = top_level_props.intersection(set(row.keys()))
            for prop in list(existing_props):
                holdings[prop] = row.pop(prop)
            if len(row) > 0:
                filtered_holdings.append(row)
        if len(filtered_holdings) > 0:
            holdings[holding_property] = filtered_holdings

    def wrapper(*args, **kwargs):
        holdings_record = args[1]
        _filter_property(holdings_record, "holdingsStatements")
        _filter_property(holdings_record, "notes")
        holdings_record["callNumber"] = "MARC Holdings"
        func(*args, **kwargs)

    return wrapper


def _ignore_coded_holdings_statements(*args):
    """
    This function overrides RulesMapperHolding method for mapping
    various 85x and 86x fields to notes and HoldingsStatements
    """
    pass


def _ignore_fix_853_bug_in_rules(*args):
    """
    This function overrides the upstream repo fix (not really a fix) for the
    852 mapping rules where the `permanentLocationId` is saved as a property
    to the holdingsStatement.note.
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


def _add_note_to_holding(row: pd.Series, holding: dict, note_types: dict):
    if "notes" not in holding.keys():
        holding["notes"] = []

    note = {"note": row["NOTE"]}
    match row["NOTE_TYPE"]:
        case "CIRCNOTE":
            note["staffOnly"] = True
            note["holdingsNoteTypeId"] = note_types.get("Circ Staff")

        case "CIRCSTAFF":
            note["staffOnly"] = True
            note["holdingsNoteTypeId"] = note_types.get("Circ Staff")

        case "HVSHELFLOC":
            note["staffOnly"] = True
            note["holdingsNoteTypeId"] = note_types.get("HVSHELFLOC")

        case "PUBLIC":
            note["staffOnly"] = False
            note["holdingsNoteTypeId"] = note_types.get("Public")

        case "TECHSTAFF":
            note["staffOnly"] = True
            note["holdingsNoteTypeId"] = note_types.get("Tech Staff")

        case _:
            note["staffOnly"] = False

    holding["notes"].append(note)


def _get_holdings_lookups(**kwargs) -> Tuple[dict, dict]:
    """
    Retrieves Holdings Note Types from FOLIO
    """
    folio_client = kwargs.get("folio_client")

    if folio_client is None:
        folio_client = FolioClient(
            Variable.get("okapi_url"),
            "sul",
            Variable.get("migration_user"),
            Variable.get("migration_password"),
        )

    note_types, holdings_types = {}, {}

    for row in folio_client.folio_get("/holdings-note-types?limit=100").get(
        'holdingsNoteTypes', []
    ):
        note_types[row["name"]] = row["id"]

    for row in folio_client.folio_get("/holdings-types?limit=100").get(
        'holdingsTypes', []
    ):
        holdings_types[row['id']] = row['name']

    return note_types, holdings_types


def _basecallnum_notes(notes_df: pd.DataFrame, holding: dict, notes_type_lookup: dict):
    for row in notes_df.loc[
        (
            (notes_df["HOMELOCATION"] == "BASECALNUM")
            | (notes_df["CURRENTLOCATION"] == "BASECALNUM")
        )
    ].iterrows():
        _add_note_to_holding(row[1], holding, notes_type_lookup)


def _electronic_notes(notes_df: pd.DataFrame, holding: dict, notes_type_lookup: dict):
    for row in notes_df.loc[notes_df["HOMELOCATION"] == "INTERNET"].iterrows():
        _add_note_to_holding(row[1], holding, notes_type_lookup)


def holdings_only_notes(**kwargs) -> None:
    """
    Creates notes for BASECALLNUM and Internet holdings that do not have FOLIO Items
    """
    airflow: str = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]

    holdingsnotes_path = pathlib.Path(kwargs["holdingsnotes_tsv"])
    iteration_dir = pathlib.Path(f"{airflow}/migration/iterations/{dag.run_id}")

    holdings_path = iteration_dir / "results/folio_holdings.json"

    holdings_note_types, holding_types = _get_holdings_lookups(**kwargs)

    with holdings_path.open() as fo:
        holdings = [json.loads(line) for line in fo.readlines()]

    if not holdingsnotes_path.exists():
        logger.info(f"{holdingsnotes_path} doesn't exist; exiting")
        return

    holdingsnote_df = pd.read_csv(holdingsnotes_path, sep="\t", dtype=object)

    logger.info(f"Addings notes from {holdingsnotes_path}")

    existing_basenums, existing_electronic = set(), set()
    for holding in holdings:
        hrid = holding["hrid"]
        hrid_base, holding_count = hrid.split("_")
        catkey = hrid_base[2:]
        holding_notes_df = holdingsnote_df.loc[holdingsnote_df["CATKEY"] == catkey]
        if len(holding_notes_df) < 1:
            continue
        if holding_count == "1" and catkey not in existing_basenums:
            _basecallnum_notes(holding_notes_df, holding, holdings_note_types)
            existing_basenums.add(catkey)
        if (
            holding_types.get(holding["holdingsTypeId"]) == "Electronic"
            and catkey not in existing_electronic
        ):
            _electronic_notes(holding_notes_df, holding, holdings_note_types)
            existing_electronic.add(catkey)

    with holdings_path.open("w+") as fo:
        for holding in holdings:
            fo.write(f"{json.dumps(holding)}\n")

    holdingsnotes_path.unlink()


def update_holdings(**kwargs):
    folio_client = kwargs.get("folio_client")

    if folio_client is None:
        folio_client = FolioClient(
            Variable.get("okapi_url"),
            "sul",
            Variable.get("migration_user"),
            Variable.get("migration_password"),
        )

    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]
    iteration_dir = pathlib.Path(f"{airflow}/migration/iterations/{dag.run_id}")
    holdings_path = iteration_dir / "results/folio_holdings.json"
    mhld_holdings_path = iteration_dir / "results/folio_holdings_mhld-transformer.json"

    srs_path = iteration_dir / "results/folio_srs_holdings_mhld-transformer.json"

    all_holdings = _generate_lookups(holdings_path)

    with (iteration_dir / "results/instance-holdings-items.json").open() as fo:
        instance_map = json.load(fo)

    locations_lookup = {}
    for location in folio_client.locations:
        locations_lookup[location["id"]] = location["code"]

    if mhld_holdings_path.exists() and srs_path.exists():
        with mhld_holdings_path.open() as fo:
            mhld_holdings = [json.loads(line) for line in fo.readlines()]

        with srs_path.open() as fo:
            srs_records = [json.loads(line) for line in fo.readlines()]

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

        mhld_holdings_path.unlink()

    with (iteration_dir / "results/folio_holdings.json").open("w+") as fo:
        for holdings_record in all_holdings.values():
            if "formerIds" in holdings_record:
                del holdings_record["formerIds"]
            for i, admin_note in enumerate(
                holdings_record.get("administrativeNotes", [])
            ):
                if admin_note.startswith("Identifier(s) from previous system"):
                    holdings_record["administrativeNotes"].pop(i)
            fo.write(f"{json.dumps(holdings_record)}\n")

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

    if not holdings_filepath.exists():
        logger.info(f"{holdings_filepath} doesn't exist; exiting")
        return

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
        fallback_holdings_type_id="f6ba0bff-5674-445b-9922-8451d0365814",
        update_hrid_settings=False,
    )

    holdings_transformer = HoldingsCsvTransformer(
        holdings_configuration, library_config, use_logging=False
    )

    _run_transformer(holdings_transformer, airflow, dag.run_id, holdings_filepath)

    _add_holdings_type_ids(
        folio_client=holdings_transformer.folio_client,
        airflow=airflow,
        dag_run_id=dag.run_id,
    )
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
        pathlib.Path(
            f"{airflow}/migration/iterations/{dag.run_id}/source_data/holdings/{filepath.name}"
        )
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
        fallback_holdings_type_id="f6ba0bff-5674-445b-9922-8451d0365814",
        create_source_records=True,
        update_hrid_settings=False,
    )

    # Overrides static method to allow duplicate CATKEYs in MHLD records
    RulesMapperHoldings.get_legacy_ids = partialmethod(
        _alt_get_legacy_ids, RulesMapperHoldings
    )

    # Overrides method that applies default mappings for 85x and 86x fields
    RulesMapperHoldings.parse_coded_holdings_statements = partialmethod(
        _ignore_coded_holdings_statements, RulesMapperHoldings
    )

    # Overrides method that handles 852 field
    RulesMapperHoldings.fix_853_bug_in_rules = partialmethod(
        _ignore_fix_853_bug_in_rules, RulesMapperHoldings
    )

    # Aligns characters to remove with mod-data-import
    Conditions.condition_remove_ending_punc = partialmethod(
        _alt_condition_remove_ending_punc
    )

    holdings_transformer = HoldingsMarcTransformer(
        mhld_holdings_config, library_config, use_logging=False
    )

    holdings_transformer.mapper.perform_additional_mapping = _wrap_additional_mapping(
        holdings_transformer.mapper.perform_additional_mapping
    )

    _run_transformer(holdings_transformer, airflow, dag.run_id, None)

    logger.info(f"Finished transforming MHLD {filepath.name} to FOLIO holdings")


def electronic_holdings(*args, **kwargs) -> None:
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
        update_hrid_settings=False,
    )

    holdings_transformer = HoldingsCsvTransformer(
        holdings_configuration, library_config, use_logging=False
    )

    _run_transformer(holdings_transformer, airflow, dag.run_id, full_path)

    logger.info(f"Finished transforming electronic {filename} to FOLIO holdings")


def boundwith_holdings(*args, **kwargs):
    okapi_url = Variable.get("okapi_url")
    folio_client = kwargs.get("folio_client")

    dag = kwargs["dag_run"]
    task_instance = kwargs["task_instance"]
    airflow = kwargs.get("airflow", "/opt/airflow")

    iteration_dir = pathlib.Path(f"{airflow}/migration/iterations/{dag.run_id}")
    bwchild_file = task_instance.xcom_pull(
        task_ids="bib-files-group", key="bwchild-file"
    )

    if bwchild_file is None:
        logger.error("Boundwidth child file does not exist, exiting")
        return
    bw_tsv_path = pathlib.Path(bwchild_file)

    holdingsnotes_file = task_instance.xcom_pull(
        task_ids="bib-files-group", key="tsv-holdingsnotes"
    )

    holdingsnotes_df = pd.read_csv(holdingsnotes_file, sep="\t", dtype=object)

    bw_holdings_json_path = iteration_dir / "results/folio_holdings_boundwith.json"
    bw_parts_json_path = iteration_dir / "results/boundwith_parts.json"

    if folio_client is None:
        folio_client = FolioClient(
            okapi_url,
            "sul",
            Variable.get("migration_user"),
            Variable.get("migration_password"),
        )

    holdings_note_types, _ = _get_holdings_lookups(folio_client=folio_client)

    locations_lookup = {}
    for location in folio_client.locations:
        if "SEE-OTHER" in location["code"]:
            locations_lookup[location["code"]] = location["id"]

    with bw_tsv_path.open(encoding="utf-8-sig") as tsv:
        logger.info("Processing boundwiths")
        bw_reader = csv.DictReader(tsv, delimiter="\t")

        with (
            bw_holdings_json_path.open("w+") as bwh,
            bw_parts_json_path.open("w+") as bwp,
        ):
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
                    "callNumber": (
                        row["BASE_CALL_NUMBER"] + " " + row["VOLUME_INFO"]
                    ).strip(),
                    "callNumberTypeId": constants.call_number_codes.get(
                        row["CALL_NUMBER_TYPE"], constants.call_number_codes["LC"]
                    ),
                    "permanentLocationId": perm_loc_id,
                    "holdingsTypeId": "5b08b35d-aaa3-4806-998c-9cd85e5bc406",
                }

                bw_holdings_notes = holdingsnotes_df.loc[
                    (
                        (holdingsnotes_df["CATKEY"] == row["CATKEY"])
                        & (holdingsnotes_df["CALL_SEQ"] == row["CALL_SEQ"])
                        & (holdingsnotes_df["COPY"] == row["COPY"])
                        & (holdingsnotes_df["ITEM_CAT1"] == "BW-CHILD")
                    )
                ]
                if len(bw_holdings_notes) > 0:
                    holdings["notes"] = []
                    for note_row in bw_holdings_notes.iterrows():
                        _add_note_to_holding(note_row[1], holdings, holdings_note_types)

                logger.info(f"Writing holdings id {holdings_id} to file")
                bwh.write(f"{json.dumps(holdings)}\n")

                if len(row["BARCODE"]) < 1:
                    continue

                bw_parts = {
                    "holdingsRecordId": holdings_id,
                    "itemId": str(
                        FolioUUID(okapi_url, FOLIONamespaces.items, row["BARCODE"])
                    ),
                }
                bwp.write(f"{json.dumps(bw_parts)}\n")

    bw_tsv_path.unlink()
