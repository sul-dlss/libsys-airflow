import json
import logging
import pathlib

from folioclient import FolioClient

from libsys_airflow.plugins.google_scanning.constants import (
    ARCHIVED_FILES_BASE,
    STAGED_FILES_BASE,
    STATUS_FAILED,
    STATUS_FILENAME,
)
from libsys_airflow.plugins.google_scanning.helpers import (
    _lookup_by_barcode,
    read_staged_barcode_files,
)
from libsys_airflow.plugins.google_scanning.staging import staged_cart_status

logger = logging.getLogger(__name__)


def barcodes_for_shipment(
    selected_carts: list[dict],
) -> tuple[list[tuple[str, str]], list[dict]]:
    """
    Reads each selected cart's staged barcode file and excludes barcodes the
    stage_cart_items DAG flags as missing or erroring during item updates.
    Returns (barcode, cart_name) pairs to include in the shipment, plus a
    per-cart skip report for the confirmation email.
    """
    to_ship: list[tuple[str, str]] = []
    skipped: list[dict] = []

    for cart in selected_carts:
        cart_name = cart["cart_name"]
        filename = cart["filename"]
        staged_file = STAGED_FILES_BASE / cart_name / filename

        barcodes = read_staged_barcode_files(str(staged_file))

        status = staged_cart_status(cart_name)
        excluded: dict[str, str] = {
            barcode: "No FOLIO item found during staging"
            for barcode in status.get("missing_barcodes", [])
        }
        excluded.update(
            {
                error["barcode"]: error.get("reason", "Unknown error during staging")
                for error in status.get("errors", [])
                if "barcode" in error
            }
        )

        for barcode in barcodes:
            if barcode in excluded:
                continue
            to_ship.append((barcode, cart_name))

        if excluded:
            skipped.append(
                {
                    "cart_name": cart_name,
                    "barcodes": [
                        {"barcode": barcode, "reason": reason}
                        for barcode, reason in sorted(excluded.items())
                    ],
                }
            )

    return to_ship, skipped


def _lookup_dereferenced_item_by_barcode(
    barcode: str, folio_client: FolioClient
) -> dict:
    return _lookup_by_barcode(
        "/item-storage-dereferenced/items", "dereferencedItems", barcode, folio_client
    )


def resolve_instance_ids(
    barcode_cart_pairs: list[tuple[str, str]], folio_client: FolioClient
) -> tuple[dict[str, str], list[dict]]:
    """
    Resolves each barcode to its instance id for building the shipment's
    MARCXML. Returns a barcode -> instance_id map plus a list of resolution
    failures for the confirmation email.
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
    counts/missing_barcodes/errors written by stage_cart_items
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


def mark_carts_failed(
    selected_carts: list[dict], shipment_dag_run_id: str | None
) -> None:
    """
    Marks every selected cart's status.json as failed. Used for any
    on_campus_shipment failure since barcodes from all
    selected carts are merged together before that point, there's no way
    to tell which cart(s) actually caused it. All selected carts stay in
    staged/ (not archived) so staff can retry once the issue's fixed.
    """
    for cart in selected_carts:
        update_cart_status(
            cart["cart_name"],
            STAGED_FILES_BASE,
            status=STATUS_FAILED,
            shipment_dag_run_id=shipment_dag_run_id,
        )


def _next_archive_dir(cart_name: str) -> pathlib.Path:
    """
    Returns the destination directory for archiving this cart. Cart names
    (booktrucks) get reused across different shipments over time, so a
    repeat gets a "_n" suffix -- "cart_name_2", "cart_name_3", etc. --
    rather than colliding with an earlier shipment's archive. The
    filesystem itself is the source of truth for which n is next; nothing
    tracks a counter anywhere else.
    """
    dest_dir = ARCHIVED_FILES_BASE / cart_name
    if not dest_dir.exists():
        return dest_dir

    n = 2
    while (ARCHIVED_FILES_BASE / f"{cart_name}_{n}").exists():
        n += 1
    return ARCHIVED_FILES_BASE / f"{cart_name}_{n}"


def archive_shipped_cart(cart_name: str) -> pathlib.Path:
    """
    Moves a shipped cart's staged directory (barcode file + status.json)
    from STAGED_FILES_BASE to ARCHIVED_FILES_BASE. Only called once a
    shipment succeeds -- carts stay in staged/ on failure so staff can
    retry.

    The destination directory's name (not status.json's own "cart_name"
    field, which still reflects the original name from staging) is what
    list_shipped_carts() displays, so a "_n"-suffixed repeat still shows up
    correctly as e.g. "Stanford001_2" in the Shipped Items table.
    """
    source_dir = STAGED_FILES_BASE / cart_name
    dest_dir = _next_archive_dir(cart_name)
    dest_dir.parent.mkdir(parents=True, exist_ok=True)
    source_dir.replace(dest_dir)
    return dest_dir
