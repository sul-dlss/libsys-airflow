import json
import logging

from datetime import datetime
from pathlib import Path

from airflow_client.client import DagRunApi, TriggerDAGRunPostBody

from libsys_airflow.plugins.google_scanning.constants import (
    ARCHIVED_FILES_BASE,
    ON_CAMPUS_SHIPMENT_DAG_ID,
    STAGE_CART_ITEMS_DAG_ID,
    STAGED_FILES_BASE,
    STATUS_FILENAME,
    STATUS_UNKNOWN,
)
from libsys_airflow.plugins.shared.airflow_api_client import api_client

logger = logging.getLogger(__name__)


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


def _cart_status(base: Path, cart_name: str) -> dict:
    """
    Returns the processing outcome for a cart under the given base directory
    (staged or archived), written by the stage_cart_items and on_campus_shipment 
    DAGs. Defaults to STATUS_UNKNOWN whenever status.json can't be read.
    """
    status_path = base / cart_name / STATUS_FILENAME
    if not status_path.exists():
        return {"status": STATUS_UNKNOWN}

    try:
        return json.loads(status_path.read_text())
    except json.JSONDecodeError:
        logger.error(f"Could not parse status file {status_path}")
        return {"status": STATUS_UNKNOWN}


def staged_cart_status(cart_name: str) -> dict:
    return _cart_status(STAGED_FILES_BASE, cart_name)


def shipped_cart_status(cart_name: str) -> dict:
    return _cart_status(ARCHIVED_FILES_BASE, cart_name)


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


def list_shipped_carts() -> list[dict]:
    """
    Lists all shipped carts, archived by the on_campus_shipment DAG
    once a shipment succeeds. Draws from ARCHIVED_FILES_BASE.
    """
    shipped_carts: list[dict] = []
    if not ARCHIVED_FILES_BASE.exists():
        return shipped_carts

    for cart_dir in sorted(ARCHIVED_FILES_BASE.iterdir()):
        if not cart_dir.is_dir():
            continue
        cart_name = cart_dir.name
        status = shipped_cart_status(cart_name)
        for archived_file in cart_dir.iterdir():
            if archived_file.name == STATUS_FILENAME:
                continue
            shipped_carts.append(
                {
                    "cart_name": cart_name,
                    "filename": archived_file.name,
                    "shipped_at": status.get("shipped_at"),
                    "status": status,
                }
            )

    return sorted(shipped_carts, key=lambda cart: cart["shipped_at"] or "")


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
    shipped_at is the staff-chosen ship date from the UI datepicker since 
    a shipment may be triggered on a different date than when the carts 
    were actually staged, reformatted to YYYYMMDD.
    """
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        trigger_body = TriggerDAGRunPostBody(
            conf={
                "selected_carts": selected_carts,
                "user_email": user_email,
                "shipped_at": datetime.strptime(shipped_at, "%Y-%m-%d").strftime(
                    "%Y%m%d"
                ),
            }
        )
        api_response = api_instance.trigger_dag_run(
            ON_CAMPUS_SHIPMENT_DAG_ID, trigger_body
        )
        return api_response.dag_run_id
