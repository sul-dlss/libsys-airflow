from pathlib import Path

STAGED_FILES_BASE = Path("/opt/airflow/data-export-files/google_scanning/staged")

# Sibling of STAGED_FILES_BASE, The on_campus_shipment DAG moves
# a cart's staged file + status.json here once shipped.
ARCHIVED_FILES_BASE = Path("/opt/airflow/data-export-files/google_scanning/archived")

# The shipment's manifest is written alongside it under "manifests" (see
# manifest.py::generate_manifest). Both are moved into a sibling
# "transmitted" directory (archive_transmitted_data_task) once uploaded.
MARC_FILES_BASE = Path("/opt/airflow/data-export-files/google_scanning/marc-files")

STAGE_CART_ITEMS_DAG_ID = "stage_cart_items"
ON_CAMPUS_SHIPMENT_DAG_ID = "on_campus_shipment"

STATUS_FILENAME = "status.json"

# Valid values for a staged cart's status.json "status" field.
# "staged" is written by the stage_cart_items DAG
# "shipped" and "failed" are written by the on_campus_shipment DAG.
STATUS_STAGED = "staged"
STATUS_SHIPPED = "shipped"
STATUS_FAILED = "failed"

# Used only by when status.json is missing or unreadable not written by processing DAGs.
STATUS_UNKNOWN = "unknown"
