import logging

from datetime import datetime
from zoneinfo import ZoneInfo

from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowException
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    archive_transmitted_data_task,
)
from libsys_airflow.plugins.google_scanning.caiasoft_api import CaiaSoftAPIWrapper
from libsys_airflow.plugins.google_scanning.caiasoft_shipment import (
    barcode_bin_pairs,
    dispatched_shipments,
    sal3_filestamp,
)
from libsys_airflow.plugins.google_scanning.drive import upload_to_drive_task
from libsys_airflow.plugins.google_scanning.email import (
    sal3_confirmation_email,
    sal3_failure_email,
    send_sal3_failure_email,
)
from libsys_airflow.plugins.google_scanning.manifest import generate_manifest
from libsys_airflow.plugins.google_scanning.marc import generate_marc_for_instances
from libsys_airflow.plugins.google_scanning.shipment import resolve_instance_ids
from libsys_airflow.plugins.shared.folio_client import folio_client

logger = logging.getLogger(__name__)

pacific_timezone = ZoneInfo("America/Los_Angeles")


def _notify_failure(context: dict) -> None:
    """
    on_failure_callback for tasks that can fail before the check_upload
    branch runs.
    """
    ti = context["task_instance"]
    dag_run = context.get("dag_run")
    reason = f"{ti.task_id} failed: {context.get('exception')}"
    send_sal3_failure_email(reason, dag_run)


@dag(
    schedule=CronDataIntervalTimetable(
        cron=Variable.get("google_scanning_caiasoft_check", "0 6 * * *"),
        timezone="America/Los_Angeles",
    ),
    catchup=False,
    start_date=datetime(2026, 8, 14),
    tags=["google_scanning", "caiasoft", "folio"],
    max_active_runs=1,
)
def google_scanning_caiasoft_sal3():

    @task(on_failure_callback=_notify_failure)
    def fetch_manifest(**kwargs) -> dict:
        data_interval_start = kwargs["data_interval_start"].astimezone(pacific_timezone)
        yesterday = data_interval_start.strftime("%Y%m%d")
        manifest = CaiaSoftAPIWrapper().courier_manifest(
            yesterday, yesterday, courier="GOOGLE"
        )
        return {
            "manifest": manifest,
            "date": yesterday,
            "dispatched": dispatched_shipments(manifest),
        }

    @task.branch(on_failure_callback=_notify_failure)
    def check_shipments(fetched: dict) -> str:
        if not fetched["dispatched"]:
            return "no_shipments"
        return "gather_barcodes"

    @task
    def no_shipments(fetched: dict) -> None:
        logger.info(f"No dispatched CaiaSoft shipments for {fetched['date']}")

    @task(on_failure_callback=_notify_failure)
    def gather_barcodes(fetched: dict) -> dict:
        shipments = fetched["dispatched"]
        pairs = barcode_bin_pairs(shipments)
        if not pairs:
            raise AirflowException(
                f"No barcodes found in dispatched CaiaSoft shipments for {fetched['date']}"
            )
        return {
            "pairs": pairs,
            "shipment_numbers": [shipment.get("shipment") for shipment in shipments],
        }

    @task(on_failure_callback=_notify_failure)
    def resolve_instances(gathered: dict) -> dict:
        folio_api_client = folio_client()
        instance_ids, failures = resolve_instance_ids(
            gathered["pairs"], folio_api_client
        )
        if not instance_ids:
            raise AirflowException("No barcodes resolved to a FOLIO instance id")
        return {"instance_ids": instance_ids, "instance_id_failures": failures}

    @task(on_failure_callback=_notify_failure)
    def generate_marc(resolved: dict, fetched: dict) -> dict:
        return generate_marc_for_instances(
            list(resolved["instance_ids"].values()), sal3_filestamp(fetched["date"])
        )

    @task(on_failure_callback=_notify_failure)
    def generate_sal3_manifest(
        gathered: dict, resolved: dict, marc_result: dict
    ) -> str:
        shipped_pairs = [
            (barcode, bin_id)
            for barcode, bin_id in gathered["pairs"]
            if barcode in resolved["instance_ids"]
        ]
        return generate_manifest(shipped_pairs, marc_result["filestamp"])

    @task(on_failure_callback=_notify_failure)
    def combine_upload_files(marc_result: dict, manifest_path: str) -> list:
        return [marc_result["marc_xml_path"], manifest_path]

    @task.branch
    def check_upload(upload_result: dict) -> list[str] | str:
        if upload_result["failures"]:
            return "mark_failed_status"
        return ["archive_transmitted_data_task", "build_sal3_result"]

    @task
    def build_sal3_result(
        gathered: dict,
        resolved: dict,
        marc_result: dict,
        manifest_path: str,
        fetched: dict,
    ) -> dict:
        return {
            "date": fetched["date"],
            "shipment_numbers": gathered["shipment_numbers"],
            "shipped_barcode_count": len(gathered["pairs"]),
            "instance_id_failures": resolved["instance_id_failures"],
            "not_found_instance_ids": marc_result["not_found_instance_ids"],
            "marc_xml_path": marc_result["marc_xml_path"],
            "manifest_path": manifest_path,
        }

    @task
    def mark_failed_status(upload_result: dict) -> str:
        return f"Failed to upload files to Google Drive: {upload_result['failures']}"

    @task
    def raise_shipment_failure(reason: str) -> None:
        raise AirflowException(reason)

    fetched = fetch_manifest()
    shipment_branch = check_shipments(fetched)
    skip = no_shipments(fetched)

    gathered = gather_barcodes(fetched)
    resolved = resolve_instances(gathered)
    marc_result = generate_marc(resolved, fetched)
    manifest_path = generate_sal3_manifest(gathered, resolved, marc_result)
    files_to_upload = combine_upload_files(marc_result, manifest_path)
    upload_result = upload_to_drive_task.override(on_failure_callback=_notify_failure)(
        files_to_upload
    )

    upload_branch = check_upload(upload_result)

    archived = archive_transmitted_data_task(upload_result["success"])
    sal3_result = build_sal3_result(
        gathered, resolved, marc_result, manifest_path, fetched
    )
    sal3_confirmation_email(sal3_result)

    failure_reason = mark_failed_status(upload_result)
    sal3_failure_email(failure_reason)
    raise_shipment_failure(failure_reason)

    shipment_branch >> [skip, gathered]
    upload_branch >> [archived, sal3_result, failure_reason]


google_scanning_caiasoft_sal3()
