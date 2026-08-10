import logging

from datetime import datetime

from airflow.sdk import dag, task, get_current_context
from airflow.exceptions import AirflowException

from libsys_airflow.plugins.google_scanning.constants import (
    STAGED_FILES_BASE,
    STATUS_SHIPPED,
)
from libsys_airflow.plugins.data_exports.transmission_tasks import (
    archive_transmitted_data_task,
)
from libsys_airflow.plugins.google_scanning.drive import upload_to_drive_task
from libsys_airflow.plugins.google_scanning.email import (
    send_shipment_failure_email,
    shipment_confirmation_email,
    shipment_failure_email,
)
from libsys_airflow.plugins.google_scanning.manifest import generate_manifest
from libsys_airflow.plugins.google_scanning.marc import generate_shipment_marc
from libsys_airflow.plugins.google_scanning.shipment import (
    archive_shipped_cart,
    barcodes_for_shipment,
    mark_carts_failed,
    resolve_instance_ids,
    update_cart_status,
)
from libsys_airflow.plugins.shared.folio_client import folio_client

logger = logging.getLogger(__name__)


def _mark_failed_and_notify(context: dict) -> None:
    """
    on_failure_callback for tasks that can fail before the check_upload
    branch runs (gather_barcodes, resolve_instances below) -- there's no
    upload result yet at that point, so the branch's own failure path
    (mark_failed_status/shipment_failure_email) never fires for these.
    Marks every selected cart failed, since barcodes from all selected
    carts are merged together before this point, so there's no way to
    tell which one(s) actually caused the failure.
    """
    ti = context["task_instance"]
    dag_run = context.get("dag_run")
    params = context.get("params", {})

    mark_carts_failed(
        params.get("selected_carts", []), getattr(dag_run, "run_id", None)
    )

    reason = f"{ti.task_id} failed: {context.get('exception')}"
    send_shipment_failure_email(reason, dag_run, params.get("user_email"))


@dag(
    schedule=None,
    catchup=False,
    start_date=datetime(2026, 8, 6),
    tags=["google_scanning", "folio"],
    max_active_runs=1,
)
def on_campus_shipment():

    @task
    def setup(**kwargs) -> dict:
        context = get_current_context()
        params = context.get("params", {})
        if "selected_carts" not in params:
            raise ValueError("Missing selected_carts")
        if "shipped_at" not in params:
            raise ValueError("Missing shipped_at")
        return {
            "selected_carts": params["selected_carts"],
            "shipped_at": params["shipped_at"],
        }

    @task(on_failure_callback=_mark_failed_and_notify)
    def gather_barcodes(init_params: dict) -> dict:
        to_ship, skipped = barcodes_for_shipment(init_params["selected_carts"])
        if not to_ship:
            raise AirflowException(
                "No barcodes available to ship after excluding barcodes "
                "already flagged missing or erroring during staging"
            )
        logger.info(
            f"{len(to_ship)} barcode(s) to ship, {len(skipped)} cart(s) with skips"
        )
        return {"to_ship": to_ship, "skipped": skipped}

    @task(on_failure_callback=_mark_failed_and_notify)
    def resolve_instances(gathered: dict) -> dict:
        folio_api_client = folio_client()
        instance_ids, failures = resolve_instance_ids(
            gathered["to_ship"], folio_api_client
        )
        if not instance_ids:
            raise AirflowException("No barcodes resolved to a FOLIO instance id")
        return {"instance_ids": instance_ids, "instance_id_failures": failures}

    @task
    def generate_marc(resolved: dict, init_params: dict) -> dict:
        return generate_shipment_marc(
            list(resolved["instance_ids"].values()), init_params["shipped_at"]
        )

    @task
    def generate_shipment_manifest(gathered: dict, marc_result: dict) -> str:
        return generate_manifest(gathered["to_ship"], marc_result["filestamp"])

    @task
    def combine_upload_files(marc_result: dict, manifest_path: str) -> list:
        return [marc_result["marc_xml_path"], manifest_path]

    @task.branch
    def check_upload(upload_result: dict) -> str:
        if upload_result["failures"]:
            return "mark_failed_status"
        return "archive_and_mark_shipped"

    @task
    def archive_and_mark_shipped(gathered: dict, init_params: dict, **kwargs) -> list:
        dag_run = kwargs["dag_run"]
        shipped_cart_names = sorted({cart_name for _, cart_name in gathered["to_ship"]})
        for cart in init_params["selected_carts"]:
            cart_name = cart["cart_name"]
            update_cart_status(
                cart_name,
                STAGED_FILES_BASE,
                status=STATUS_SHIPPED,
                shipped_at=init_params["shipped_at"],
                shipment_dag_run_id=dag_run.run_id,
            )
            archive_shipped_cart(cart_name)
        logger.info(f"Archived {len(shipped_cart_names)} shipped cart(s)")
        return shipped_cart_names

    @task
    def build_shipment_result(
        shipped_carts: list,
        gathered: dict,
        resolved: dict,
        marc_result: dict,
        manifest_path: str,
    ) -> dict:
        return {
            "shipped_carts": shipped_carts,
            "shipped_barcode_count": len(gathered["to_ship"]),
            "skipped": gathered["skipped"],
            "instance_id_failures": resolved["instance_id_failures"],
            "not_found_instance_ids": marc_result["not_found_instance_ids"],
            "marc_xml_path": marc_result["marc_xml_path"],
            "manifest_path": manifest_path,
        }

    @task
    def mark_failed_status(upload_result: dict, init_params: dict, **kwargs) -> str:
        dag_run = kwargs["dag_run"]
        mark_carts_failed(init_params["selected_carts"], dag_run.run_id)
        return f"Failed to upload files to Google Drive: {upload_result['failures']}"

    @task
    def raise_shipment_failure(reason: str) -> None:
        raise AirflowException(reason)

    init_params = setup()
    gathered = gather_barcodes(init_params)
    resolved = resolve_instances(gathered)
    marc_result = generate_marc(resolved, init_params)
    manifest_path = generate_shipment_manifest(gathered, marc_result)
    files_to_upload = combine_upload_files(marc_result, manifest_path)
    upload_result = upload_to_drive_task(files_to_upload)

    upload_branch = check_upload(upload_result)

    shipped_carts = archive_and_mark_shipped(gathered, init_params)
    archived_shipment_files = archive_transmitted_data_task(upload_result["success"])
    shipment_result = build_shipment_result(
        shipped_carts, gathered, resolved, marc_result, manifest_path
    )
    shipment_confirmation_email(shipment_result)

    failure_reason = mark_failed_status(upload_result, init_params)
    shipment_failure_email(failure_reason)
    raise_shipment_failure(failure_reason)

    upload_branch >> [shipped_carts, archived_shipment_files, failure_reason]


on_campus_shipment()
