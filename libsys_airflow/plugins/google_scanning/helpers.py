import datetime
import json
import logging
import pathlib

from folioclient import FolioClient

from libsys_airflow.plugins.google_scanning.constants import (
    STATUS_FAILED,
    STATUS_FILENAME,
    STATUS_STAGED,
)

logger = logging.getLogger(__name__)


def _lookup_item_by_barcode(barcode, folio_client: FolioClient) -> dict:
    """
    Lookup and return Item by barcode
    """
    try:
        item_result = folio_client.folio_get(
            "/inventory/items", key="items", query=f"barcode=={barcode}"
        )
    except Exception as e:
        return {"barcode": barcode, "reason": f"{e} for barcode: {barcode}"}

    output = {}
    match len(item_result):

        case 0:
            output = {"missing": barcode}

        case 1:
            output = item_result[0]

        case _:
            output = {
                "barcode": barcode,
                "reason": f"multiple items found for barcode: {barcode}",
            }
    return output


def _update_item_for_shipment(**kwargs) -> dict:
    """
    Updates item's temp_location_id, note_type, and statistical code
    """
    item: dict = kwargs["item"]
    folio_client: FolioClient = kwargs["folio_client"]
    temp_location_id: str = kwargs["temp_location_id"]
    digi_sent_id: str = kwargs["digi_sent_id"]
    note_type_id: str = kwargs["note_type_id"]
    date: str = kwargs["date"]
    item["temporaryLocationId"] = temp_location_id
    if digi_sent_id not in item["statisticalCodeIds"]:
        item["statisticalCodeIds"].append(digi_sent_id)
    item["notes"].append(
        {
            "itemNoteTypeId": note_type_id,
            "note": f"Sent for Google scanning/{date}",
            "staffOnly": True,
        }
    )
    output = {}
    try:
        folio_client.folio_put(f"/inventory/items/{item['id']}", payload=item)
    except Exception as e:
        output["barcode"] = item["barcode"]
        output["reason"] = (
            f"{item['id']} with barcode: {item['barcode']} failed to update, error: {e}"
        )
    return output


def get_folio_uuids(folio_client: FolioClient, temp_location_code) -> tuple:
    """
    Returns tuple of FOLIO UUIDs
    """
    temp_location_uuids = [
        row["id"]
        for row in folio_client.locations
        if row["code"].startswith(temp_location_code)
    ]
    if len(temp_location_uuids) != 1:
        raise ValueError(
            f"Missing temp location code or too many locations codes for {temp_location_code}"
        )
    temp_location_id = temp_location_uuids[0]

    digi_sent_uuids = [
        row["id"]
        for row in folio_client.statistical_codes
        if row['code'].startswith("DIGI-SENT")
    ]
    if len(digi_sent_uuids) != 1:
        raise ValueError("Missing DIGI-SENT stat code or too many matches")
    digi_sent_id = digi_sent_uuids[0]

    item_note_types = folio_client.folio_get(
        "/item-note-types", key="itemNoteTypes", query="name==Reproduction"
    )
    if len(item_note_types) != 1:
        raise ValueError("Missing Item Note types or too many matches")
    note_type_id = item_note_types[0].get("id")
    return temp_location_id, digi_sent_id, note_type_id


def process_barcode(**kwargs) -> dict:
    """
    Processes Item retrieved from barcode
    """
    barcode: str = kwargs["barcode"]
    folio_client: FolioClient = kwargs["folio_client"]

    lookup_result = _lookup_item_by_barcode(barcode, folio_client)
    if "id" not in lookup_result:  # Errors do not have the item uuid
        return lookup_result
    kwargs["item"] = lookup_result
    kwargs["date"] = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d")
    return _update_item_for_shipment(**kwargs)


def read_staged_barcode_files(staged_file: str) -> list:
    """
    Reads Staged Barcode File and returns a list of barcodes
    """
    barcode_file_path = pathlib.Path(staged_file)
    if not barcode_file_path.exists():
        raise FileNotFoundError(f"{staged_file} does not exist")
    barcodes = []
    for row in barcode_file_path.read_text().splitlines():
        barcode = row.strip()
        if len(barcode) == 0:
            continue
        barcodes.append(barcode)
    return barcodes


def write_status_json(
    init_params: dict, total_barcodes: int, update_results: dict
) -> bool:
    """
    Writes status.json file of updating barcodes next staged barcodes file
    """
    barcode_file_path = pathlib.Path(init_params["staged_file_path"])
    status_json_path = barcode_file_path.parent / STATUS_FILENAME
    # No barcodes actually updated -- every one was missing or errored --
    # means this cart never got staged successfully at all, so flag it as
    # failed rather than staged. Staff can then re-address the cart instead
    # of it silently sitting in the staged list looking normal.
    status_value = (
        STATUS_FAILED if update_results["successful_updates"] == 0 else STATUS_STAGED
    )
    status = {
        "cart_name": init_params["cart_name"],
        "staged_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "total_barcodes": total_barcodes,
        "updated": update_results["successful_updates"],
        "missing_barcodes": update_results["missing"],
        "errors": update_results["errors"],
        "status": status_value,
        "shipped_at": None,
        "shipment_dag_run_id": None,
    }
    with status_json_path.open("w+") as fo:
        try:
            json.dump(status, fo, indent=2)
        except Exception as e:
            logger.error(f"Failed to save {status_json_path}, error: {e}")
            return False
    return True
