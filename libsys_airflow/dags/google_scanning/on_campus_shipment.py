import logging

from datetime import datetime

from airflow.sdk import dag, task, get_current_context
from airflow.exceptions import AirflowException

from libsys_airflow.plugins.google_scanning.email import (
    send_shipment_failure_email,
    shipment_confirmation_email,
)
from libsys_airflow.plugins.google_scanning.manifest import generate_manifest
from libsys_airflow.plugins.google_scanning.marc import generate_marc_for_instances
from libsys_airflow.plugins.google_scanning.shipment import (
    barcodes_for_shipment,
    mark_carts_failed,
    plan_shipment,
    queue_shipment,
    read_shipment,
    resolve_instance_ids,
)
from libsys_airflow.plugins.google_scanning.staging import shipment_filestamp
from libsys_airflow.plugins.shared.folio_client import folio_client

logger = logging.getLogger(__name__)


def _mark_failed_and_notify(context: dict) -> None:
    """
    on_failure_callback for every task after setup. If there are no barcodes
    available in FOLIO or none that resolve to an instance, marks every
    selected cart failed, since barcodes from all selected carts are merged
    together before this point, so there is no way to tell which one(s)
    actually caused the failure. A shipment left "building" (see
    shipment.py::plan_shipment) isn't sent until it's triggered again.
    """
    ti = context["task_instance"]
    dag_run = context.get("dag_run")
    params = context.get("params", {})

    mark_carts_failed(
        params.get("selected_carts", []), getattr(dag_run, "run_id", None)
    )

    reason = f"{ti.task_id} failed: {context.get('exception')}"
    if shipped_at := params.get("shipped_at"):
        stem = shipment_filestamp(shipped_at)
        if read_shipment(stem):
            reason += (
                f" The {stem} shipment won't be sent to Google Drive until "
                "it's triggered again."
            )
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
    def plan(init_params: dict, **kwargs) -> dict:
        return plan_shipment(
            init_params["selected_carts"],
            init_params["shipped_at"],
            kwargs["dag_run"].run_id,
        )

    @task(on_failure_callback=_mark_failed_and_notify)
    def gather_barcodes(shipment_plan: dict) -> dict:
        to_ship, skipped = barcodes_for_shipment(shipment_plan["carts"])
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

    @task(on_failure_callback=_mark_failed_and_notify)
    def generate_marc(resolved: dict, shipment_plan: dict) -> dict:
        return generate_marc_for_instances(
            list(resolved["instance_ids"].values()), shipment_plan["stem"]
        )

    @task(on_failure_callback=_mark_failed_and_notify)
    def generate_shipment_manifest(
        gathered: dict, resolved: dict, marc_result: dict
    ) -> str:
        shipped_pairs = [
            (barcode, cart_name)
            for barcode, cart_name in gathered["to_ship"]
            if barcode in resolved["instance_ids"]
        ]
        return generate_manifest(shipped_pairs, marc_result["filestamp"])

    @task(on_failure_callback=_mark_failed_and_notify)
    def queue_carts(
        shipment_plan: dict,
        gathered: dict,
        init_params: dict,
        manifest_path: str,
        **kwargs,
    ) -> list:
        # manifest_path is only taken so this runs after the manifest is
        # written, since the shipment is marked "ready" here.
        shipped_cart_names = sorted({cart_name for _, cart_name in gathered["to_ship"]})
        shipment_carts = queue_shipment(
            shipment_plan,
            shipped_cart_names,
            init_params["shipped_at"],
            kwargs["dag_run"].run_id,
        )
        logger.info(
            f"Queued {shipment_plan['stem']} with {len(shipment_carts)} cart(s) "
            "for the send_google_scanning_files DAG"
        )
        return shipment_carts

    @task
    def build_shipment_result(
        shipment_carts: list,
        shipment_plan: dict,
        gathered: dict,
        resolved: dict,
        marc_result: dict,
        manifest_path: str,
    ) -> dict:
        return {
            "shipped_carts": shipment_carts,
            "rebuild": shipment_plan["rebuild"],
            "stem": shipment_plan["stem"],
            "shipped_barcode_count": len(gathered["to_ship"]),
            "skipped": gathered["skipped"],
            "instance_id_failures": resolved["instance_id_failures"],
            "not_found_instance_ids": marc_result["not_found_instance_ids"],
            "marc_xml_path": marc_result["marc_xml_path"],
            "manifest_path": manifest_path,
        }

    init_params = setup()
    shipment_plan = plan(init_params)
    gathered = gather_barcodes(shipment_plan)
    resolved = resolve_instances(gathered)
    marc_result = generate_marc(resolved, shipment_plan)
    manifest_path = generate_shipment_manifest(gathered, resolved, marc_result)
    shipment_carts = queue_carts(shipment_plan, gathered, init_params, manifest_path)
    shipment_result = build_shipment_result(
        shipment_carts, shipment_plan, gathered, resolved, marc_result, manifest_path
    )
    shipment_confirmation_email(shipment_result)


on_campus_shipment()
