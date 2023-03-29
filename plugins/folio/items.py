import csv
import json
import logging
import pathlib

import pandas as pd
import requests

from folioclient import FolioClient

from folio_migration_tools.migration_tasks.items_transformer import ItemsTransformer

from plugins.folio.helpers import post_to_okapi, setup_data_logging

logger = logging.getLogger(__name__)


def _generate_item_notes(
    item, tsv_note_df: pd.DataFrame, item_note_types: dict
) -> list:
    """Takes TSV notes dataframe and returns a list of generated Item notes"""
    barcode = item.get("barcode")
    if barcode is None:
        logger.error("Item missing barcode, cannot generate notes")
        return
    item_notes = tsv_note_df.loc[tsv_note_df["BARCODE"] == barcode]

    # Drop any notes that do not have a value
    item_notes = item_notes.dropna(subset=["note"])

    notes = []
    for row in item_notes.iterrows():
        note_info = row[1]
        note = {"note": note_info["note"]}

        match note_info["NOTE_TYPE"]:
            case "CIRCNOTE":
                note["staffOnly"] = True
                note["itemNoteTypeId"] = item_note_types.get("Circ Staff")
                notes.append(note)

            case "CIRCSTAFF":
                note["staffOnly"] = True
                note["itemNoteTypeId"] = item_note_types.get("Circ Staff")
                notes.append(note)

            case "HVSHELFLOC":
                note["staffOnly"] = True
                note["itemNoteTypeId"] = item_note_types.get("HVSHELFLOC")
                notes.append(note)

            case "PUBLIC":
                note["staffOnly"] = False
                note["itemNoteTypeId"] = item_note_types.get("Public")
                notes.append(note)

            case "TECHSTAFF":
                note["staffOnly"] = True
                note["itemNoteTypeId"] = item_note_types.get("Tech Staff")
                notes.append(note)

    if len(notes) > 0:
        item["notes"] = notes


def _statistical_codes_lookup(airflow: str, folio_client: FolioClient) -> dict:
    """
    Constructs Item statistical lookup dictionary for handling multiple
    Item stat codes
    """
    item_code_type_result = requests.get(
        f"{folio_client.okapi_url}/statistical-code-types?query=name==Item&limit=200",
        headers=folio_client.okapi_headers)
    item_code_type_result.raise_for_status()
    item_code_type = item_code_type_result.json()['statisticalCodeTypes'][0]['id']
    item_stat_codes_result = requests.get(
        f"{folio_client.okapi_url}/statistical-codes?query=statisticalCodeTypeId=={item_code_type}&limit=200",
        headers=folio_client.okapi_headers)
    item_stat_codes_result.raise_for_status()
    folio_code_ids = {}
    for row in item_stat_codes_result.json()['statisticalCodes']:
        folio_code_ids[row['code']] = row['id']
    enf_stat_result = requests.get(
        f"{folio_client.okapi_url}/statistical-codes?query=code=ENF",
        headers=folio_client.okapi_headers)
    enf_stat_result.raise_for_status()
    folio_code_ids["ENF"] = enf_stat_result.json()['statisticalCodes'][0]['id']
    item_stat_codes = {}
    with open(airflow / "migration/mapping_files/statcodes.csv") as fo:
        stat_code_reader = csv.DictReader(fo, delimiter="\t")
        for row in stat_code_reader:
            item_stat_codes[row["ITEM_CAT1"]] = folio_code_ids[row["folio_code"]]
    return item_stat_codes


def _retrieve_item_notes_ids(folio_client) -> dict:
    """Retrieves itemNoteTypes from Okapi"""
    note_types = dict()
    note_types_response = requests.get(
        f"{folio_client.okapi_url}/item-note-types?limit=100",
        headers=folio_client.okapi_headers,
    )

    if note_types_response.status_code > 399:
        raise ValueError(
            f"Cannot retrieve item note types from {folio_client.okapi_url}\n{note_types_response.text}"
        )

    for note_type in note_types_response.json()["itemNoteTypes"]:
        note_types[note_type["name"]] = note_type["id"]

    return note_types


def _add_additional_info(**kwargs):
    """Generates notes from tsv files"""
    airflow: str = kwargs["airflow"]
    items_pattern: str = kwargs["items_pattern"]
    tsv_notes_path = kwargs["tsv_notes_path"]
    folio_client = kwargs["folio_client"]
    dag_run_id: str = kwargs["dag_run_id"]

    barcode_lookup = _suppressed_conditions(airflow, dag_run_id)

    results_dir = pathlib.Path(f"{airflow}/migration/iterations/{dag_run_id}/results")

    if tsv_notes_path is not None:
        tsv_notes_path = pathlib.Path(tsv_notes_path)
        tsv_notes_df = pd.read_csv(tsv_notes_path, sep="\t", dtype=object)

        item_note_types = _retrieve_item_notes_ids(folio_client)

    items = []

    for items_file in results_dir.glob(items_pattern):
        logger.info(f"Processing {items_file}")
        with items_file.open() as fo:
            for line in fo.readlines():
                item = json.loads(line)
                item["_version"] = 1
                if tsv_notes_path is not None:
                    _generate_item_notes(item, tsv_notes_df, item_note_types)
                _set_discovery_suppress(item, barcode_lookup)
                items.append(item)

                if not len(items) % 1000:
                    logger.info(f"Updated {len(items):,} item records")

        with open(items_file, "w+") as write_output:
            for item in items:
                write_output.write(f"{json.dumps(item)}\n")


def _set_discovery_suppress(item, barcode_lookup):
    if barcode_lookup.get(item.get("barcode"), False):
        item["discoverySuppress"] = True


def _suppressed_conditions(airflow, dag_run_id):
    """
    Creates a lookup dictionary for barcodes that value is a boolean
    """
    migration_dir = pathlib.Path(f"{airflow}/migration")
    source_dir = migration_dir / f"iterations/{dag_run_id}/source_data/items"
    item_tsv = None
    for tsv_file in source_dir.glob("*.tsv"):
        if "electronic" in tsv_file.name or "notes" in tsv_file.name:
            continue
        item_tsv = tsv_file

    if item_tsv is None:
        logger.error(f"No item tsv file found in {source_dir}")
        return {}

    with (migration_dir / "mapping_files/items-suppressed-locations.json").open() as fo:
        suppressed_locations = json.load(fo)

    barcode_lookup = {}
    with item_tsv.open() as fo:
        item_reader = csv.DictReader(fo, delimiter="\t")
        for row in item_reader:
            barcode_lookup[row["BARCODE"]] = False
            if row["CURRENTLOCATION"] in suppressed_locations:
                barcode_lookup[row["BARCODE"]] = True
            if row["HOMELOCATION"] in suppressed_locations:
                barcode_lookup[row["BARCODE"]] = True
            if row["ITEM_SHADOW"] == "1":
                barcode_lookup[row["BARCODE"]] = True
            if row["CALL_SHADOW"] == "1":
                barcode_lookup[row["BARCODE"]] = True
    return barcode_lookup


def post_folio_items_records(**kwargs):
    """Creates/overlays Items records in FOLIO"""
    dag = kwargs["dag_run"]

    batch_size = int(kwargs.get("MAX_ENTITIES", 1000))
    job_number = kwargs.get("job")

    with open(f"/tmp/items-{dag.run_id}-{job_number}.json") as fo:
        items_records = json.load(fo)

    for i in range(0, len(items_records), batch_size):
        items_batch = items_records[i:i + batch_size]
        logger.info(f"Posting {len(items_batch)} in batch {i/batch_size}")
        post_to_okapi(
            token=kwargs["task_instance"].xcom_pull(
                key="return_value", task_ids="post-to-folio.folio_login"
            ),
            records=items_batch,
            endpoint="/item-storage/batch/synchronous?upsert=true",
            payload_key="items",
            **kwargs,
        )


def run_items_transformer(*args, **kwargs) -> bool:
    """Runs item tranformer"""
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]
    instance = kwargs["task_instance"]
    library_config = kwargs["library_config"]

    library_config.iteration_identifier = dag.run_id

    items_stem = kwargs["items_stem"]

    if items_stem.startswith("ON-ORDER"):
        mapping_file = "item_mapping_on_order.json"
    else:
        mapping_file = "item_mapping.json"

    item_config = ItemsTransformer.TaskConfiguration(
        name="transformer",
        migration_task_type="ItemsTransformer",
        hrid_handling="preserve001",
        files=[{"file_name": f"{items_stem}.tsv", "suppress": False}],
        items_mapping_file_name=mapping_file,
        location_map_file_name="locations.tsv",
        temp_location_map_file_name="temp_locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        material_types_map_file_name="material_types.tsv",
        loan_types_map_file_name="loan_types.tsv",
        statistical_codes_map_file_name="statcodes.tsv",
        item_statuses_map_file_name="item_statuses.tsv",
        call_number_type_map_file_name="call_number_type_mapping.tsv",
    )

    items_transformer = ItemsTransformer(item_config, library_config, use_logging=False)

    setup_data_logging(items_transformer)

    items_transformer.do_work()

    items_transformer.wrap_up()

    _add_additional_info(
        airflow=airflow,
        dag_run_id=dag.run_id,
        items_pattern="folio_items_*transformer.json",
        tsv_notes_path=instance.xcom_pull(
            task_ids="move-transform.symphony-tsv-processing", key="tsv-notes"
        ),
        folio_client=items_transformer.folio_client
    )
