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


@task
def upload_to_drive_task(file_list: list) -> dict:
    """
    Uploads files to the Google Drive folder shared with Google for scanning
    shipments, via the libsys_drive Connection.
    Returns lists of files successfully uploaded and failures, following the
    transmit-data task shape in plugins/data_exports/transmission_tasks.py.
    """
    if not is_production():
        logger.info("SKIPPING GOOGLE DRIVE UPLOAD")
        return {"success": file_list, "failures": []}

    success = []
    failures = []
    folder_id = drive_folder_id()
    hook = GoogleDriveHook(gcp_conn_id=DRIVE_CONN_ID)
    for f in file_list:
        remote_location = Path(f).name
        try:
            logger.info(f"Start upload of file {f} to Google Drive")
            hook.upload_file(
                local_location=f,
                remote_location=remote_location,
                folder_id=folder_id,
            )
            success.append(f)
            logger.info(f"Uploaded {f} to Google Drive")
        except Exception as e:
            logger.error(f"Error uploading {f} to Google Drive: {e}")
            failures.append(f)

    return {"success": success, "failures": failures}
