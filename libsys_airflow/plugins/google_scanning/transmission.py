import logging
import shutil

from collections import defaultdict
from pathlib import Path

from libsys_airflow.plugins.google_scanning.caiasoft_shipment import is_sal3_stem
from libsys_airflow.plugins.google_scanning.constants import (
    ARCHIVED_FILES_BASE,
    CARTS_WAITING_BASE,
    SAL3_ARCHIVE_NAME,
    SHIPMENT_READY,
)
from libsys_airflow.plugins.google_scanning.shipment import read_shipment
from libsys_airflow.plugins.google_scanning.staging import stem_ship_date, timestamp

logger = logging.getLogger(__name__)


def sendable_shipment_files(file_list: list[str], send_date: str) -> dict:
    """
    Filters gather_files_task's file list down to shipments that are ready
    to send: both the MARCXML (new/) and manifest (manifests/) are present,
    and, for on-campus shipments, shipment.json is "ready" -- so a
    shipment that's being rebuilt, or whose rebuild failed, is never sent
    half-written. Shipments dated after send_date (YYYYMMDD) are held
    until then. Returns the files to upload, the held stems, and the
    skipped shipments and why, for the failure email.
    """
    by_stem: dict[str, dict[str, str]] = defaultdict(dict)
    for file in file_list:
        path = Path(file)
        by_stem[path.stem][path.parent.name] = file

    to_send: list[str] = []
    held: list[str] = []
    skipped: list[dict] = []
    for stem, files in sorted(by_stem.items()):
        ship_date = stem_ship_date(stem)
        if ship_date and ship_date > send_date:
            logger.info(f"Holding {stem} until {ship_date}")
            held.append(stem)
            continue
        if "new" not in files:
            skipped.append({"stem": stem, "reason": "missing its MARCXML file"})
            continue
        if "manifests" not in files:
            skipped.append({"stem": stem, "reason": "missing its manifest file"})
            continue
        if not is_sal3_stem(stem):
            shipment = read_shipment(stem)
            if shipment is None:
                skipped.append(
                    {
                        "stem": stem,
                        "reason": "has no waiting shipment; it may already "
                        "have been sent",
                    }
                )
                continue
            if shipment.get("status") != SHIPMENT_READY:
                skipped.append(
                    {
                        "stem": stem,
                        "reason": "still being built, or its last build failed; "
                        "trigger the shipment again to send it",
                    }
                )
                continue
        to_send.extend([files["new"], files["manifests"]])

    return {"file_list": to_send, "held": held, "skipped": skipped}


def _copy_shipment_files(files: list[Path], dest_dir: Path) -> None:
    for shipment_file in files:
        shutil.copy2(shipment_file, dest_dir / shipment_file.name)


def archive_sent_shipments(sent_files: list[str]) -> list[str]:
    """
    Keeps permanent copies of each sent shipment under archived/. Runs
    before move_transmitted_data_task, while the files are still in
    marc-files/. Each on-campus cart gets copies of the shipment's MARCXML
    and manifest, then moves from carts-waiting/{stem}/{cart}/ to
    archived/{cart}/{timestamp}/; SAL3 shipments have no carts and are
    copied to archived/sal3/{stem}/. Safe to re-run after a failure: carts
    already archived are no longer in carts-waiting/. Returns the archive
    directories written.
    """
    files_by_stem: dict[str, list[Path]] = defaultdict(list)
    for file in sent_files:
        files_by_stem[Path(file).stem].append(Path(file))

    run_timestamp = timestamp()
    archived: list[str] = []
    for stem, files in sorted(files_by_stem.items()):
        if is_sal3_stem(stem):
            dest_dir = ARCHIVED_FILES_BASE / SAL3_ARCHIVE_NAME / stem
            dest_dir.mkdir(parents=True, exist_ok=True)
            _copy_shipment_files(files, dest_dir)
            archived.append(str(dest_dir))
            logger.info(f"Archived {stem} to {dest_dir}")
            continue

        shipment_dir = CARTS_WAITING_BASE / stem
        if not shipment_dir.is_dir():
            logger.info(f"{stem} was already archived")
            continue
        cart_dirs = sorted(path for path in shipment_dir.iterdir() if path.is_dir())
        for cart_dir in cart_dirs:
            # Copy first, so a cart only reaches archived/ with its shipment files
            _copy_shipment_files(files, cart_dir)
            dest_dir = ARCHIVED_FILES_BASE / cart_dir.name / run_timestamp
            dest_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(cart_dir, dest_dir)
            archived.append(str(dest_dir))
        shutil.rmtree(shipment_dir)
        logger.info(f"Archived {stem} to {len(cart_dirs)} cart folder(s)")

    return archived
