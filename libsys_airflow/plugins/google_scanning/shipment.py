import json
import logging
import pathlib
import shutil

from folioclient import FolioClient

from libsys_airflow.plugins.google_scanning.constants import (
    BUILDING_SUFFIX,
    CARTS_WAITING_BASE,
    MARC_FILES_BASE,
    SHIPMENT_BUILDING,
    SHIPMENT_FILENAME,
    SHIPMENT_READY,
    STAGED_FILES_BASE,
    STATUS_FAILED,
    STATUS_FILENAME,
    STATUS_SHIPPED,
)
from libsys_airflow.plugins.google_scanning.helpers import (
    _lookup_by_barcode,
    read_staged_barcode_files,
)
from libsys_airflow.plugins.google_scanning.manifest import manifest_filename
from libsys_airflow.plugins.google_scanning.staging import (
    barcode_filename,
    cart_status_in,
    other_date_conflicts,
    shipment_filestamp,
)

logger = logging.getLogger(__name__)


def barcodes_for_shipment(
    shipment_carts: list[dict],
) -> tuple[list[tuple[str, str]], list[dict]]:
    """
    Reads each shipment cart's barcode file (from staged/ or, for a
    rebuild, carts-waiting/ -- see plan_shipment) and excludes barcodes the
    stage_cart_items DAG flags as missing or erroring during item updates.
    Returns (barcode, cart_name) pairs to include in the shipment, plus a
    per-cart skip report for the confirmation email.
    """
    to_ship: list[tuple[str, str]] = []
    skipped: list[dict] = []

    for cart in shipment_carts:
        cart_name = cart["cart_name"]
        cart_dir = pathlib.Path(cart["cart_dir"])

        barcodes = read_staged_barcode_files(str(cart_dir / cart["filename"]))

        status = cart_status_in(cart_dir)
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
    to tell which cart(s) actually caused it. Selected carts stay in
    staged/ so staff can retry once the issue's fixed -- except any that
    queue_shipment already moved to carts-waiting/ before it failed.
    """
    for cart in selected_carts:
        if not (STAGED_FILES_BASE / cart["cart_name"]).is_dir():
            continue
        update_cart_status(
            cart["cart_name"],
            STAGED_FILES_BASE,
            status=STATUS_FAILED,
            shipment_dag_run_id=shipment_dag_run_id,
        )


def read_shipment(stem: str) -> dict | None:
    """
    Returns a waiting shipment's shipment.json, or None if there's no
    waiting shipment for that stem.
    """
    shipment_path = CARTS_WAITING_BASE / stem / SHIPMENT_FILENAME
    if not shipment_path.exists():
        return None
    return json.loads(shipment_path.read_text())


def write_shipment(stem: str, shipment: dict) -> None:
    shipment_dir = CARTS_WAITING_BASE / stem
    shipment_dir.mkdir(parents=True, exist_ok=True)
    with (shipment_dir / SHIPMENT_FILENAME).open("w") as fo:
        json.dump(shipment, fo, indent=2)


def building_filestamp(stem: str) -> str:
    """
    Returns the filestamp a build writes its MARCXML, manifest and
    instance ids under, until queue_shipment renames them to the stem.
    """
    return f"{stem}{BUILDING_SUFFIX}"


def shipment_files(filestamp: str) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
    """
    Returns the MARCXML, manifest and instance ids paths that
    marc.py::generate_marc_for_instances and manifest.py::generate_manifest
    write for a filestamp.
    """
    return (
        MARC_FILES_BASE / "new" / f"{filestamp}.xml",
        MARC_FILES_BASE / "manifests" / manifest_filename(filestamp),
        MARC_FILES_BASE.parent / "instanceids" / "new" / f"{filestamp}.csv",
    )


def _move_cart_into_shipment(cart_name: str, dest_dir: pathlib.Path) -> None:
    """
    Moves a staged cart to dest_dir, replacing a re-shipped cart's earlier
    copy only once the move succeeds, so a failed move leaves it in place.
    """
    replaced_dir = dest_dir.with_name(f".{cart_name}.replaced")
    if dest_dir.exists():
        dest_dir.rename(replaced_dir)
    try:
        shutil.move(STAGED_FILES_BASE / cart_name, dest_dir)
    except Exception:
        if replaced_dir.exists():
            replaced_dir.rename(dest_dir)
        raise
    if replaced_dir.exists():
        shutil.rmtree(replaced_dir)


def plan_shipment(selected_carts: list[dict], shipped_at: str) -> dict:
    """
    Works out which carts go into the ship date's on-campus shipment.
    There's one per date, so if that date's shipment is still waiting to
    be sent, this rebuilds it: its carts keep their order, any that are
    being re-shipped point at their new staged/ copy, and newly selected
    carts go on the end. Changes nothing on disk, so if the build fails,
    a waiting shipment is still sent as it was.
    """
    conflicts = other_date_conflicts(
        [cart["cart_name"] for cart in selected_carts], shipped_at
    )
    if conflicts:
        raise ValueError(" ".join(conflicts))

    stem = shipment_filestamp(shipped_at)
    staged = {
        cart["cart_name"]: {
            "cart_name": cart["cart_name"],
            "filename": cart["filename"],
            "cart_dir": str(STAGED_FILES_BASE / cart["cart_name"]),
            "source": "staged",
        }
        for cart in selected_carts
    }

    shipment_carts = []
    existing = read_shipment(stem)
    for cart_name in (existing or {}).get("carts", []):
        if cart_name in staged:
            shipment_carts.append(staged.pop(cart_name))
            continue
        cart_dir = CARTS_WAITING_BASE / stem / cart_name
        # A failed earlier build may list a staged cart that never moved here
        if not cart_dir.is_dir():
            continue
        shipment_carts.append(
            {
                "cart_name": cart_name,
                "filename": barcode_filename(cart_dir),
                "cart_dir": str(cart_dir),
                "source": "waiting",
            }
        )
    shipment_carts.extend(staged.values())

    return {"stem": stem, "rebuild": existing is not None, "carts": shipment_carts}


def queue_shipment(
    plan: dict,
    shipped_cart_names: list[str],
    shipped_at: str,
    dag_run_id: str | None,
) -> list[str]:
    """
    Moves each staged cart that shipped into carts-waiting/{stem}/{cart}/,
    replacing a re-shipped cart's earlier copy, renames the build's files
    from building_filestamp to the stem, then marks the shipment "ready"
    for the send DAG. The shipment is "building" while this runs, so the
    send DAG skips it if this fails partway, until it's triggered again.
    A staged cart with no shippable barcodes is left staged and dropped
    from the shipment (along with any earlier copy). Returns the
    shipment's cart names.
    """
    stem = plan["stem"]
    shipment = {
        "stem": stem,
        "shipped_at": shipped_at,
        "carts": [cart["cart_name"] for cart in plan["carts"]],
        "status": SHIPMENT_BUILDING,
        "dag_run_id": dag_run_id,
    }
    write_shipment(stem, shipment)

    shipment_cart_names = []
    for cart in plan["carts"]:
        cart_name = cart["cart_name"]
        if cart["source"] == "waiting":
            shipment_cart_names.append(cart_name)
            continue
        dest_dir = CARTS_WAITING_BASE / stem / cart_name
        if cart_name not in shipped_cart_names:
            if dest_dir.exists():
                shutil.rmtree(dest_dir)
            continue
        update_cart_status(
            cart_name,
            STAGED_FILES_BASE,
            status=STATUS_SHIPPED,
            shipped_at=shipped_at,
            shipment_stem=stem,
            shipment_dag_run_id=dag_run_id,
        )
        _move_cart_into_shipment(cart_name, dest_dir)
        shipment_cart_names.append(cart_name)

    for built, final in zip(
        shipment_files(building_filestamp(stem)), shipment_files(stem)
    ):
        built.replace(final)

    shipment.update({"carts": shipment_cart_names, "status": SHIPMENT_READY})
    write_shipment(stem, shipment)
    return shipment_cart_names
