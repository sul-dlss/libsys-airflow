import logging

from pathlib import Path

from airflow.sdk import task, Variable
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

from libsys_airflow.plugins.shared.utils import is_production

logger = logging.getLogger(__name__)

DRIVE_CONN_ID = "libsys_drive"

# Default "Metadata & Manifests" folder shared with the libsys service account.
# https://drive.google.com/drive/u/1/folders/1Mov-oJfN6yJmgMY6h93RrqtevbSS_Kt6
DEFAULT_DRIVE_FOLDER_ID = "1Mov-oJfN6yJmgMY6h93RrqtevbSS_Kt6"


def drive_folder_id() -> str:
    return Variable.get("GOOGLE_SCANNING_DRIVE_FOLDER_ID", DEFAULT_DRIVE_FOLDER_ID)


def _rollback_uploads(hook: GoogleDriveHook, uploaded: list[tuple[str, str]]) -> None:
    """
    Best-effort delete of files already uploaded in a batch that ended up
    with at least one failure, so a partial failure doesn't leave an
    orphaned file behind in Drive.
    """
    service = hook.get_conn()
    for f, file_id in uploaded:
        try:
            service.files().delete(fileId=file_id, supportsAllDrives=True).execute()
            logger.info(f"Rolled back partial upload: deleted {f} (Drive id {file_id})")
        except Exception as e:
            logger.error(
                f"Failed to roll back {f} (Drive id {file_id}) after partial "
                f"upload failure: {e}"
            )


@task
def upload_to_drive_task(file_list: list) -> dict:
    """
    Uploads files to the Google Drive folder shared with Google for scanning
    shipments, via the libsys_drive Connection.
    Returns lists of files successfully uploaded and failures, following the
    transmit-data task shape in plugins/data_exports/transmission_tasks.py.
    If any file in the batch fails, rolls back any files from this batch
    that already uploaded, so success is all-or-nothing for the batch.
    """
    if not is_production():
        logger.info("SKIPPING GOOGLE DRIVE UPLOAD")
        return {"success": file_list, "failures": []}

    uploaded: list[tuple[str, str]] = []
    failures = []
    folder_id = drive_folder_id()
    hook = GoogleDriveHook(gcp_conn_id=DRIVE_CONN_ID)
    for f in file_list:
        remote_location = Path(f).name
        try:
            logger.info(f"Start upload of file {f} to Google Drive")
            file_id = hook.upload_file(
                local_location=f,
                remote_location=remote_location,
                folder_id=folder_id,
            )
            uploaded.append((f, file_id))
            logger.info(f"Uploaded {f} to Google Drive")
        except Exception as e:
            logger.error(f"Error uploading {f} to Google Drive: {e}")
            failures.append(f)

    if failures and uploaded:
        _rollback_uploads(hook, uploaded)
        uploaded = []

    return {"success": [f for f, _ in uploaded], "failures": failures}
