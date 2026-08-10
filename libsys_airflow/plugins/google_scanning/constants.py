from pathlib import Path

STAGED_FILES_BASE = Path("/opt/airflow/data-export-files/google_scanning/staged")
# Sibling of STAGED_FILES_BASE, following the archive_transmitted_data_task
# pattern in plugins/data_exports/transmission_tasks.py (a "transmitted"
# directory alongside "marc-files"). The on_campus_shipment DAG (#1847) moves
# a cart's staged file + status.json here once shipped.
ARCHIVED_FILES_BASE = Path("/opt/airflow/data-export-files/google_scanning/archived")

STAGE_CART_ITEMS_DAG_ID = "stage_cart_items"
ON_CAMPUS_SHIPMENT_DAG_ID = "on_campus_shipment"

STATUS_FILENAME = "status.json"

# Valid values for a staged cart's status.json "status" field.
# "staged" is written by the stage_cart_items DAG (#1852);
# "shipped" and "failed" are written by the on_campus_shipment DAG (#1847).
STATUS_STAGED = "staged"
STATUS_SHIPPED = "shipped"
STATUS_FAILED = "failed"
# Used only by this view, when status.json is missing or unreadable — never
# written by processing DAGs.
STATUS_UNKNOWN = "unknown"
