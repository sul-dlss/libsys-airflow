import logging

from datetime import datetime, timedelta

from airflow.sdk import dag, task, Variable
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    gather_files_task,
    move_transmitted_data_task,
)
from libsys_airflow.plugins.google_scanning.drive import upload_to_drive_task
from libsys_airflow.plugins.google_scanning.email import transmission_failure_email
from libsys_airflow.plugins.google_scanning.staging import pacific_today
from libsys_airflow.plugins.google_scanning.transmission import (
    archive_sent_shipments,
    sendable_shipment_files,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        cron=Variable.get("transmit_google_scanning", "0 21 * * *"),
        timezone="America/Los_Angeles",
    ),
    start_date=datetime(2026, 10, 26),
    catchup=False,
    tags=["google_scanning", "data export"],
    max_active_runs=1,
)
def send_google_scanning_files():
    """
    Uploads the on-campus and SAL3 shipments' MARCXML and manifests that
    the on_campus_shipment and google_scanning_caiasoft_sal3 DAGs left in
    marc-files/ to Google Drive, keeps permanent copies under archived/,
    then moves them to transmitted/.
    """

    @task
    def select_sendable_files(gathered: dict) -> dict:
        return sendable_shipment_files(
            gathered["file_list"], pacific_today().strftime("%Y%m%d")
        )

    # Only skip the upload, so transmission_failure_email still reports any
    # skipped shipments.
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def has_files_to_send(sendable: dict) -> bool:
        if not sendable["file_list"]:
            logger.info("No shipments ready to send")
            return False
        return True

    @task
    def archive_shipments(upload_result: dict) -> list:
        return archive_sent_shipments(upload_result["success"])

    gathered = gather_files_task(vendor="google_scanning")
    sendable = select_sendable_files(gathered)
    check_files = has_files_to_send(sendable)
    upload_result = upload_to_drive_task(sendable["file_list"])
    archived = archive_shipments(upload_result)
    moved = move_transmitted_data_task(upload_result["success"])

    check_files >> upload_result
    archived >> moved
    transmission_failure_email(upload_result, sendable)


send_google_scanning_files()
