import datetime
import pathlib

from folioclient import FolioClient


def _lookup_item_by_barcode(barcode, folio_client: FolioClient) -> dict:
    """
    Lookup and return Item by barcode
    """
    try:
        item_result = folio_client.folio_get(
            "/inventory/items", key="items", query=f"barcode=={barcode}"
        )
    except Exception as e:
        return {"error": f"{e} for barcode: {barcode}"}

    output = {}
    match len(item_result):

        case 0:
            output = {"error": f"not found barcode: {barcode}"}

        case 1:
            output = item_result[0]

        case _:
            output = {"error": f"multiple items found for barcode: {barcode}"}
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
    if not digi_sent_id in item["statisticalCodeIds"]:
        item["statisticalCodeIds"].append(digi_sent_id)
    item["notes"].append(
        {
            "itemNoteTypeId": note_type_id,
            "note": f"Sent to Google on {date}",
            "staffOnly": True,
        }
    )
    output = {}
    try:
        folio_client.folio_put(f"/inventory/items/{item['id']}", payload=item)
    except Exception as e:
        output["error"] = (
            f"{item['id']} with barcode: {item['barcode']} failed to update, error: {e}"
        )
    return output


def process_barcode(**kwargs) -> dict:
    """
    Processes Item retrieved from barcode
    """
    barcode: str = kwargs["barcode"]
    folio_client: FolioClient = kwargs["folio_client"]

    lookup_item = _lookup_item_by_barcode(barcode, folio_client)
    if "error" in lookup_item:
        return lookup_item
    kwargs["item"] = lookup_item
    kwargs["date"] = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    return _update_item_for_shipment(**kwargs)


def read_staged_barcode_files(barcode_file: str) -> list:
    """
    Reads Staged Barcode File and returns a list of barcodes
    """
    barcode_file_path = pathlib.Path(barcode_file)
    if not barcode_file_path.exists():
        raise FileNotFoundError(f"{barcode_file} does not exist")
    barcodes = []
    for row in barcode_file_path.read_text().splitlines():
        barcode = row.strip()
        if len(barcode) == 0:
            continue
        barcodes.append(barcode)
    return barcodes