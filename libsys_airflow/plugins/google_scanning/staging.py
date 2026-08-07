import json
import logging

from datetime import datetime
from pathlib import Path

from airflow_client.client import DagRunApi, TriggerDAGRunPostBody

from libsys_airflow.plugins.shared.airflow_api_client import api_client

logger = logging.getLogger(__name__)

STAGED_FILES_BASE = Path("/opt/airflow/data-export-files/google_scanning/staged")

STAGE_CART_ITEMS_DAG_ID = "stage_cart_items"
ON_CAMPUS_SHIPMENT_DAG_ID = "on_campus_shipment"

STATUS_FILENAME = "status.json"


def save_staged_file(cart_name: str, filename: str, contents: bytes) -> Path:
    """
    Saves an uploaded cart's barcode file to
    data-export-files/google_scanning/staged/{cart_name}/{filename}
    """
    cart_dir = STAGED_FILES_BASE / cart_name
    cart_dir.mkdir(parents=True, exist_ok=True)
    staged_file_path = cart_dir / filename
    staged_file_path.write_bytes(contents)
    return staged_file_path


def staged_cart_status(cart_name: str) -> dict:
    """
    Returns the processing outcome for a staged cart, written by the
    stage_cart_items DAG (#1852). Defaults to "Pending" if that DAG hasn't
    run yet (or doesn't exist yet).
    """
    status_path = STAGED_FILES_BASE / cart_name / STATUS_FILENAME
    if not status_path.exists():
        return {"status": "Pending"}

    try:
        return json.loads(status_path.read_text())
    except json.JSONDecodeError:
        logger.error(f"Could not parse status file {status_path}")
        return {"status": "Unknown"}


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


def trigger_stage_cart_items_dag(staged_file_path: str, cart_name: str) -> str:
    """
    Triggers the stage_cart_items DAG (#1852) to update FOLIO items for a
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
    selected_carts: list[dict], user_email: str | None
) -> str:
    """
    Triggers the on_campus_shipment DAG (#1847) for the selected staged
    carts.
    """
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        trigger_body = TriggerDAGRunPostBody(
            conf={"selected_carts": selected_carts, "user_email": user_email}
        )
        api_response = api_instance.trigger_dag_run(
            ON_CAMPUS_SHIPMENT_DAG_ID, trigger_body
        )
        return api_response.dag_run_id
