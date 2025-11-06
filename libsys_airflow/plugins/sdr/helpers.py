import logging
import pathlib

import pandas as pd

from airflow.models import Variable
from folioclient import FolioClient

logger = logging.getLogger(__name__)


def check_update_item(
    barcode: str, folio_client: FolioClient, stat_code_lookup: dict
) -> dict:
    """
    Checks SDR FOLIO Item and updates statisitical codes if needed. Returns an empty
    dict if successful or a dict with an error key for reporting.
    """
    try:
        item_result = folio_client.folio_get(
            "/inventory/items", key="items", query=f"barcode=={barcode}"
        )
    except Exception as e:
        return {"error": f"{e} for barcode: {barcode}"}

    match len(item_result):

        case 0:
            return {"error": f"not found barcode: {barcode}"}

        case 1:
            item = item_result[0]

        case _:
            return {"error": f"multiple items found for barcode: {barcode}"}

    stat_codes, update_item = [], False
    for stat_code in item["statisticalCodeIds"]:
        if stat_code in stat_code_lookup["REMOVE"]:
            update_item = True
            continue
        stat_codes.append(stat_code)
    if len(set(stat_code_lookup["ADD"]).difference(set(stat_codes))) > 0:
        stat_codes.extend(stat_code_lookup["ADD"])
        update_item = True
    if update_item:
        item["statisticalCodeIds"] = stat_codes
        try:
            folio_client.folio_put(f"/inventory/items/{item['id']}", payload=item)
        except Exception as e:
            return {"error": f"{e} for barcode: {barcode}"}
    return {}


def delete_barcode_csv(csv_file: str):
    """
    Deletes barcode csv file if present.
    """
    csv_path = pathlib.Path(csv_file)
    if csv_path.exists():
        csv_path.unlink()


def extract_barcodes(csv_file: str) -> list:
    """
    Extracts barcodes from a csv file and returns a list of batched barcodes
    """
    csv_path = pathlib.Path(csv_file)

    if not csv_path.exists():
        raise ValueError(f"{csv_file} doesn't exist")

    csv_df = pd.read_csv(csv_path, dtype=object)

    if "barcode" not in csv_df.columns:
        raise ValueError("Column barcode required in csv file")

    batch_size = int(Variable.get("SDR_ITEM_BATCH_SIZE", "10000"))

    barcode_batches = []
    for i in range(0, len(csv_df), batch_size):
        barcode_batch = csv_df["barcode"].iloc[i : i + batch_size].to_list()
        barcode_batch = list(set(barcode_batch))  # Removes any duplicate barcodes
        barcode_batches.append(barcode_batch)

    return barcode_batches


def stat_codes_lookup(folio_client: FolioClient) -> dict:
    """
    Returns a dict with 'REMOVE' and 'ADD' keys that are lists of stat code UUIDs
    that need to be removed or added respectively.
    """
    lookup: dict = {"ADD": [], "REMOVE": []}
    for stat_code in folio_client.statistical_codes:
        match stat_code["code"]:
            case "DIGI-SDR":
                lookup["ADD"].append(stat_code['id'])

            case "DIGI-SCAN" | "DIGI-SENT":
                lookup["REMOVE"].append(stat_code['id'])
    return lookup
