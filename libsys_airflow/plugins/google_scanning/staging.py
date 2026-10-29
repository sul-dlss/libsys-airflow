import json
import logging
import re
import shutil

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from airflow_client.client import DagRunApi, TriggerDAGRunPostBody

from libsys_airflow.plugins.google_scanning.constants import (
    ARCHIVED_FILES_BASE,
    CARTS_WAITING_BASE,
    MARC_FILES_BASE,
    ON_CAMPUS_SHIPMENT_DAG_ID,
    SAL3_ARCHIVE_NAME,
    STAGE_CART_ITEMS_DAG_ID,
    STAGED_FILES_BASE,
    STATUS_FILENAME,
    STATUS_UNKNOWN,
)
from libsys_airflow.plugins.shared.airflow_api_client import api_client
from libsys_airflow.plugins.shared.dag_runs import (
    active_dag_runs as shared_active_dag_runs,
)

logger = logging.getLogger(__name__)

# Base directory download paths are relative to; downloads are limited to
# its marc-files/, carts-waiting/ and archived/ subdirectories.
GOOGLE_SCANNING_BASE = STAGED_FILES_BASE.parent

TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S"

SHIPMENT_STEM_PATTERN = re.compile(r"^stanford_(\d{8})-")


def timestamp() -> str:
    return datetime.now().strftime(TIMESTAMP_FORMAT)


def is_cart_staged(cart_name: str) -> bool:
    return (STAGED_FILES_BASE / cart_name).is_dir()


def staged_cart_dir(cart_name: str) -> Path:
    """
    Returns staged/{cart_name}/, raising ValueError if the name would
    resolve anywhere else (e.g. "../pod" or "/tmp/x").
    """
    cart_dir = STAGED_FILES_BASE / cart_name
    if cart_dir.resolve().parent != STAGED_FILES_BASE.resolve():
        raise ValueError(f"Invalid cart name {cart_name!r}")
    return cart_dir


def is_staged_file_path(staged_file_path: str) -> bool:
    """
    Whether a path is a file in a cart directory directly under staged/,
    as save_staged_file writes.
    """
    return (
        Path(staged_file_path).resolve().parent.parent == STAGED_FILES_BASE.resolve()
    )


def save_staged_file(cart_name: str, filename: str, contents: bytes) -> Path:
    """
    Saves an uploaded cart's barcode file to
    data-export-files/google_scanning/staged/{cart_name}/{stem}_{timestamp}{suffix}.
    Cart names are unique among staged carts, so re-staging a cart replaces
    its whole directory (barcode file and status.json).
    """
    cart_dir = staged_cart_dir(cart_name)
    if cart_dir.exists():
        shutil.rmtree(cart_dir)
    cart_dir.mkdir(parents=True)
    filename_path = Path(filename)
    staged_file_path = (
        cart_dir / f"{filename_path.stem}_{timestamp()}{filename_path.suffix}"
    )
    staged_file_path.write_bytes(contents)
    return staged_file_path


def barcode_filename(cart_dir: Path) -> str:
    """
    Returns the name of a cart directory's barcode file, i.e. the one file
    in it that isn't status.json or, in an archived cart's folder, a copy
    of its shipment's MARCXML/manifest (named for status.json's
    shipment_stem).
    """
    stem = cart_status_in(cart_dir).get("shipment_stem")
    skip = {STATUS_FILENAME, f"{stem}.xml", f"{stem}.txt"}
    for path in sorted(cart_dir.iterdir()):
        if path.name not in skip:
            return path.name
    raise FileNotFoundError(f"No barcode file in {cart_dir}")


def staged_barcode_filename(cart_name: str) -> str:
    """
    Returns a staged cart's barcode filename, raising FileNotFoundError if
    the cart isn't staged (or its name points outside staged/).
    """
    try:
        cart_dir = staged_cart_dir(cart_name)
    except ValueError as e:
        raise FileNotFoundError(str(e)) from e
    return barcode_filename(cart_dir)


def cart_status_in(cart_dir: Path) -> dict:
    """
    Returns the processing outcome for a cart directory (staged,
    carts-waiting, or archived), written by the stage_cart_items and
    on_campus_shipment DAGs. Defaults to STATUS_UNKNOWN whenever
    status.json can't be read.
    """
    status_path = cart_dir / STATUS_FILENAME
    if not status_path.exists():
        return {"status": STATUS_UNKNOWN}

    try:
        return json.loads(status_path.read_text())
    except json.JSONDecodeError:
        logger.error(f"Could not parse status file {status_path}")
        return {"status": STATUS_UNKNOWN}


def staged_cart_status(cart_name: str) -> dict:
    return cart_status_in(STAGED_FILES_BASE / cart_name)


def waiting_shipment_for(cart_name: str) -> str | None:
    """
    Returns the stem of the shipment a cart is waiting in to be sent to
    Drive, or None if it isn't waiting in one.
    """
    if not CARTS_WAITING_BASE.exists():
        return None
    for shipment_dir in sorted(CARTS_WAITING_BASE.iterdir()):
        if (shipment_dir / cart_name).is_dir():
            return shipment_dir.name
    return None


def shipment_filestamp(shipped_at: str) -> str:
    """
    Builds the on-campus shipment's file stem for its MARCXML/manifest,
    e.g. "stanford_20260810-oncampus". There's one on-campus shipment per
    ship date: triggering another for the same date before it's sent
    rebuilds that shipment (see shipment.py::plan_shipment).
    """
    return f"stanford_{shipped_at}-oncampus"


def stem_ship_date(stem: str) -> str | None:
    """
    Returns the YYYYMMDD ship date from an on-campus or SAL3 shipment's
    file stem (see shipment_filestamp and caiasoft_shipment.py::sal3_filestamp).
    """
    match = SHIPMENT_STEM_PATTERN.match(stem)
    return match.group(1) if match else None


def pacific_today() -> date:
    return datetime.now(ZoneInfo("America/Los_Angeles")).date()


def other_date_conflicts(cart_names: list[str], shipped_at: str) -> list[str]:
    """
    Returns an error message for each cart that's already waiting in a
    different ship date's shipment. Such a cart can't be re-shipped until
    that shipment is sent, since there's one on-campus shipment per date.
    """
    stem = shipment_filestamp(shipped_at)
    conflicts = []
    for cart_name in cart_names:
        waiting_stem = waiting_shipment_for(cart_name)
        if waiting_stem and waiting_stem != stem:
            conflicts.append(
                f"{cart_name} is waiting in the {waiting_stem} shipment, "
                "which hasn't been sent to Google Drive yet."
            )
    return conflicts


def shipped_file_path(rel_path: str) -> Path:
    """
    Resolves a shipped cart's file for download, rejecting any path (e.g.
    with "..") that would resolve outside marc-files/, carts-waiting/ or
    archived/.
    """
    file_path = (GOOGLE_SCANNING_BASE / rel_path).resolve()
    for base in (MARC_FILES_BASE, CARTS_WAITING_BASE, ARCHIVED_FILES_BASE):
        if base.resolve() in file_path.parents:
            return file_path
    raise ValueError(f"Invalid shipped file path: {rel_path}")


def download_filename(file_path: Path) -> str:
    """
    Returns the filename to present for download. A cart's barcode file is
    always given a .csv extension -- it's a single column of barcodes,
    effectively a one-column CSV, regardless of the extension it was
    originally uploaded with. The shipment's MARCXML and manifest (in
    marc-files/, or named for the cart's shipment_stem) keep their names.
    """
    if MARC_FILES_BASE.resolve() in file_path.resolve().parents:
        return file_path.name
    if file_path.stem == cart_status_in(file_path.parent).get("shipment_stem"):
        return file_path.name
    return file_path.with_suffix(".csv").name


def list_staged_carts() -> list[dict]:
    """
    Lists all currently staged carts for the review/shipment page.
    """
    staged_carts: list[dict] = []
    if not STAGED_FILES_BASE.exists():
        return staged_carts

    for cart_dir in sorted(STAGED_FILES_BASE.iterdir()):
        if not cart_dir.is_dir():
            continue
        cart_name = cart_dir.name
        for staged_file in cart_dir.iterdir():
            if staged_file.name == STATUS_FILENAME:
                continue
            staged_carts.append(
                {
                    "cart_name": cart_name,
                    "filename": staged_file.name,
                    "uploaded_at": datetime.fromtimestamp(
                        staged_file.stat().st_mtime
                    ).isoformat(),
                    "status": staged_cart_status(cart_name),
                }
            )

    return sorted(staged_carts, key=lambda cart: cart["uploaded_at"])


def _shipped_cart_row(cart_name: str, cart_dir: Path, shipment_dir: Path) -> dict:
    """
    Builds a Shipped Items row with download links for the cart's barcode
    file and its shipment's MARCXML and manifest -- found in marc-files/
    while the shipment waits to be sent, and copied into the cart's
    archived folder once it has been.
    """
    status = cart_status_in(cart_dir)
    files = []
    try:
        files.append(cart_dir / barcode_filename(cart_dir))
    except FileNotFoundError:
        logger.warning(f"No barcode file in {cart_dir}")
    if stem := status.get("shipment_stem"):
        if shipment_dir == MARC_FILES_BASE:
            files.append(MARC_FILES_BASE / "new" / f"{stem}.xml")
            files.append(MARC_FILES_BASE / "manifests" / f"{stem}.txt")
        else:
            files.append(shipment_dir / f"{stem}.xml")
            files.append(shipment_dir / f"{stem}.txt")
    return {
        "cart_name": cart_name,
        "shipped_at": status.get("shipped_at"),
        "status": status,
        "files": [
            {
                "path": str(path.relative_to(GOOGLE_SCANNING_BASE)),
                "download_filename": download_filename(path),
            }
            for path in files
            if path.exists()
        ],
    }


def list_shipped_carts() -> list[dict]:
    """
    Lists each shipped cart's latest shipment: the one it's waiting in to
    be sent to Drive (carts-waiting/), otherwise its newest
    archived/{cart}/{timestamp}/ folder. A cart moves here as soon as its
    shipment is triggered, and its row looks the same once the shipment
    has been sent. Earlier shipments stay archived for auditing but aren't
    listed.
    """
    rows: dict[str, dict] = {}

    if CARTS_WAITING_BASE.exists():
        for shipment_dir in sorted(CARTS_WAITING_BASE.iterdir()):
            try:
                cart_dirs = sorted(shipment_dir.iterdir())
            except FileNotFoundError:
                # Removed by archive_sent_shipments while the page loads
                continue
            for cart_dir in cart_dirs:
                if not cart_dir.is_dir():
                    continue
                rows[cart_dir.name] = _shipped_cart_row(
                    cart_dir.name, cart_dir, MARC_FILES_BASE
                )

    if ARCHIVED_FILES_BASE.exists():
        for archive_dir in sorted(ARCHIVED_FILES_BASE.iterdir()):
            cart_name = archive_dir.name
            if (
                cart_name == SAL3_ARCHIVE_NAME
                or cart_name in rows
                or not archive_dir.is_dir()
            ):
                continue
            shipments = sorted(path for path in archive_dir.iterdir() if path.is_dir())
            if shipments:
                rows[cart_name] = _shipped_cart_row(
                    cart_name, shipments[-1], shipments[-1]
                )

    return sorted(rows.values(), key=lambda cart: cart["shipped_at"] or "")


def cart_has_active_run(cart_name: str) -> bool:
    """
    Whether a queued/running stage_cart_items or on_campus_shipment DAG run
    is processing this cart, in which case it mustn't be re-staged yet.
    """
    return any(cart_name in run["progress_keys"] for run in active_dag_runs())


def trigger_stage_cart_items_dag(staged_file_path: str, cart_name: str) -> str:
    """
    Triggers the stage_cart_items DAG to update FOLIO items for a
    newly staged cart.
    """
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        trigger_body = TriggerDAGRunPostBody(
            conf={"staged_file_path": staged_file_path, "cart_name": cart_name}
        )
        api_response = api_instance.trigger_dag_run(
            STAGE_CART_ITEMS_DAG_ID, trigger_body
        )
        return api_response.dag_run_id


def trigger_on_campus_shipment_dag(
    selected_carts: list[dict], user_email: str | None, shipped_at: str
) -> str:
    """
    Triggers the on_campus_shipment DAG for the selected staged carts.
    shipped_at (YYYYMMDD) is the shipment's date: today, from the upload
    page. The send DAG sends each date's shipment after that date ends.
    """
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        trigger_body = TriggerDAGRunPostBody(
            conf={
                "selected_carts": selected_carts,
                "user_email": user_email,
                "shipped_at": shipped_at,
            }
        )
        api_response = api_instance.trigger_dag_run(
            ON_CAMPUS_SHIPMENT_DAG_ID, trigger_body
        )
        return api_response.dag_run_id


def _dag_run_cart_names(dag_id: str, conf: dict | None) -> list[str]:
    conf = conf or {}
    if dag_id == STAGE_CART_ITEMS_DAG_ID:
        return [conf["cart_name"]] if conf.get("cart_name") else []
    return [cart["cart_name"] for cart in conf.get("selected_carts", [])]


def active_dag_runs() -> list[dict]:
    """
    Lists queued/running stage_cart_items and on_campus_shipment DAG runs
    with their task progress, for the upload page's progress indicators.
    Progress counts task instances, so a mapped task (process_barcodes_batch)
    only adds to the total once it expands.
    """
    runs = shared_active_dag_runs((STAGE_CART_ITEMS_DAG_ID, ON_CAMPUS_SHIPMENT_DAG_ID))
    for run in runs:
        run["progress_keys"] = _dag_run_cart_names(run["dag_id"], run.pop("conf"))
    return runs
