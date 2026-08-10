import json
import logging
import pathlib

from folioclient import FolioClient

from libsys_airflow.plugins.google_scanning.constants import (
    ARCHIVED_FILES_BASE,
    STAGED_FILES_BASE,
    STATUS_FILENAME,
)
from libsys_airflow.plugins.google_scanning.helpers import read_staged_barcode_files
from libsys_airflow.plugins.google_scanning.staging import staged_cart_status

logger = logging.getLogger(__name__)


def barcodes_for_shipment(
    selected_carts: list[dict],
) -> tuple[list[tuple[str, str]], list[dict]]:
    """
    Reads each selected cart's staged barcode file and excludes barcodes the
    stage_cart_items DAG (#1852) already flagged as missing or erroring
    during item updates -- there's no valid FOLIO item to resolve an
    instance id from for those. Returns (barcode, cart_name) pairs to
    include in the shipment, plus a per-cart skip report for the
    confirmation email.
    """
    to_ship: list[tuple[str, str]] = []
    skipped: list[dict] = []

    for cart in selected_carts:
        cart_name = cart["cart_name"]
        filename = cart["filename"]
        staged_file = STAGED_FILES_BASE / cart_name / filename

        barcodes = read_staged_barcode_files(str(staged_file))

        status = staged_cart_status(cart_name)
        excluded = set(status.get("missing_barcodes", []))
        excluded.update(
            error["barcode"] for error in status.get("errors", []) if "barcode" in error
        )

        for barcode in barcodes:
            if barcode in excluded:
                continue
            to_ship.append((barcode, cart_name))

        if excluded:
            skipped.append({"cart_name": cart_name, "barcodes": sorted(excluded)})

    return to_ship, skipped


def _lookup_dereferenced_item_by_barcode(
    barcode: str, folio_client: FolioClient
) -> dict:
    """
    Looks up an item by barcode via item-storage-dereferenced, which embeds
    the full instance record on the item so the shipment's instance id can
    be read directly off the result, without a separate holdings-storage
    lookup.
    https://s3.amazonaws.com/foliodocs/api/mod-inventory-storage/p/item-storage-dereferenced.html
    """
    try:
        items = folio_client.folio_get(
            "/item-storage-dereferenced/items",
            key="dereferencedItems",
            query=f"barcode=={barcode}",
        )
    except Exception as e:
        return {"barcode": barcode, "reason": f"{e} for barcode: {barcode}"}

    match len(items):
        case 0:
            return {"missing": barcode}
        case 1:
            return items[0]
        case _:
            return {
                "barcode": barcode,
                "reason": f"multiple items found for barcode: {barcode}",
            }


def resolve_instance_ids(
    barcode_cart_pairs: list[tuple[str, str]], folio_client: FolioClient
) -> tuple[dict[str, str], list[dict]]:
    """
    Resolves each barcode to its instance id for building the shipment's
    MARCXML, via item-storage-dereferenced. Returns a barcode ->
    instance_id map plus a list of resolution failures for the confirmation
    email.
    """
    instance_ids: dict[str, str] = {}
    failures: list[dict] = []

    for barcode, cart_name in barcode_cart_pairs:
        item = _lookup_dereferenced_item_by_barcode(barcode, folio_client)
        if "id" not in item:
            reason = item.get("reason", f"missing barcode: {barcode}")
            failures.append(
                {"barcode": barcode, "cart_name": cart_name, "reason": reason}
            )
            continue

        instance_record = item.get("instanceRecord")
        if not instance_record or "id" not in instance_record:
            failures.append(
                {
                    "barcode": barcode,
                    "cart_name": cart_name,
                    "reason": f"item {item['id']} has no instanceRecord",
                }
            )
            continue

        instance_ids[barcode] = instance_record["id"]

    return instance_ids, failures


def update_cart_status(cart_name: str, base: pathlib.Path, **fields) -> None:
    """
    Partial-updates an existing cart's status.json in place, preserving the
    counts/missing_barcodes/errors written by stage_cart_items (#1852),
    rather than overwriting the whole file the way write_status_json does
    for a fresh staging run.
    """
    status_path = base / cart_name / STATUS_FILENAME
    status = {}
    if status_path.exists():
        try:
            status = json.loads(status_path.read_text())
        except json.JSONDecodeError:
            logger.error(f"Could not parse status file {status_path}")

    status.update(fields)

    with status_path.open("w") as fo:
        json.dump(status, fo, indent=2)


def archive_shipped_cart(cart_name: str) -> pathlib.Path:
    """
    Moves a shipped cart's staged directory (barcode file + status.json)
    from STAGED_FILES_BASE to ARCHIVED_FILES_BASE, following the
    archive_transmitted_data_task pattern in
    plugins/data_exports/transmission_tasks.py. Only called once a shipment
    succeeds -- carts stay in staged/ on failure so staff can retry.

    Assumes a cart name isn't shipped more than once: if ARCHIVED_FILES_BASE
    already has a directory for this cart_name, the move raises rather than
    silently overwriting or merging a prior shipment's files.
    """
    source_dir = STAGED_FILES_BASE / cart_name
    dest_dir = ARCHIVED_FILES_BASE / cart_name
    dest_dir.parent.mkdir(parents=True, exist_ok=True)
    source_dir.replace(dest_dir)
    return dest_dir
