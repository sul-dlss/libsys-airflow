import logging
import shutil

from collections import defaultdict
from pathlib import Path

from libsys_airflow.plugins.google_scanning.constants import (
    ARCHIVED_FILES_BASE,
    CARTS_WAITING_BASE,
    SAL3_ARCHIVE_NAME,
    SHIPMENT_READY,
)
from libsys_airflow.plugins.google_scanning.shipment import read_shipment
from libsys_airflow.plugins.google_scanning.staging import timestamp

logger = logging.getLogger(__name__)


def sendable_shipment_files(file_list: list[str]) -> dict:
    """
    Filters gather_files_task's file list down to shipments that are ready
    to send: both the MARCXML (new/) and manifest (manifests/) are present,
    and, for on-campus shipments, shipment.json is "ready" -- so a
    shipment that's being rebuilt, or whose rebuild failed, is never sent
    half-written. Returns the files to upload plus the skipped shipments
    and why, for the failure email.
    """
    by_stem: dict[str, dict[str, str]] = defaultdict(dict)
    for file in file_list:
        path = Path(file)
        by_stem[path.stem][path.parent.name] = file

    to_send: list[str] = []
    skipped: list[dict] = []
    for stem, files in sorted(by_stem.items()):
        if "new" not in files:
            skipped.append({"stem": stem, "reason": "missing its MARCXML file"})
            continue
        if "manifests" not in files:
            skipped.append({"stem": stem, "reason": "missing its manifest file"})
            continue
        shipment = read_shipment(stem)
        if shipment and shipment.get("status") != SHIPMENT_READY:
            skipped.append(
                {
                    "stem": stem,
                    "reason": "still being built, or its last build failed; "
                    "trigger the shipment again to send it",
                }
            )
            continue
        to_send.extend([files["new"], files["manifests"]])

    return {"file_list": to_send, "skipped": skipped}


def archive_sent_shipments(sent_files: list[str]) -> list[str]:
    """
    Keeps permanent copies of each sent shipment under archived/. Runs
    before move_transmitted_data_task, while the files are still in
    marc-files/. Each on-campus cart moves from carts-waiting/{stem}/{cart}/
    to archived/{cart}/{timestamp}/ with copies of the shipment's MARCXML
    and manifest; SAL3 shipments have no carts and are copied to
    archived/sal3/{stem}/. Returns the archive directories written.
    """
    files_by_stem: dict[str, list[Path]] = defaultdict(list)
    for file in sent_files:
        files_by_stem[Path(file).stem].append(Path(file))

    run_timestamp = timestamp()
    archived: list[str] = []
    for stem, files in sorted(files_by_stem.items()):
        shipment = read_shipment(stem)
        if shipment is None:
            dest_dirs = [ARCHIVED_FILES_BASE / SAL3_ARCHIVE_NAME / stem]
        else:
            dest_dirs = []
            for cart_name in shipment.get("carts", []):
                cart_dir = CARTS_WAITING_BASE / stem / cart_name
                if not cart_dir.is_dir():
                    logger.warning(f"{cart_dir} missing; not archived")
                    continue
                dest_dir = ARCHIVED_FILES_BASE / cart_name / run_timestamp
                dest_dir.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(cart_dir, dest_dir)
                dest_dirs.append(dest_dir)

        for dest_dir in dest_dirs:
            dest_dir.mkdir(parents=True, exist_ok=True)
            for shipment_file in files:
                shutil.copy2(shipment_file, dest_dir / shipment_file.name)
            archived.append(str(dest_dir))

        if shipment is not None:
            shutil.rmtree(CARTS_WAITING_BASE / stem)
        logger.info(f"Archived {stem} to {len(dest_dirs)} folder(s)")

    return archived
