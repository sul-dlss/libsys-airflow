from pathlib import Path

STAGED_FILES_BASE = Path("/opt/airflow/data-export-files/google_scanning/staged")
# Sibling of STAGED_FILES_BASE, following the archive_transmitted_data_task
# pattern in plugins/data_exports/transmission_tasks.py (a "transmitted"
# directory alongside "marc-files"). The on_campus_shipment DAG (#1847) moves
# a cart's staged file + status.json here once shipped.
ARCHIVED_FILES_BASE = Path("/opt/airflow/data-export-files/google_scanning/archived")

# Directory marc_for_instances/add_holdings_items_to_marc_files/
# clean_and_serialize_marc_files (plugins/data_exports/marc/) write the
# on_campus_shipment DAG's (#1847) MARCXML into, under "new" -- the
# shipment's manifest is written alongside it there too.
MARC_FILES_BASE = Path("/opt/airflow/data-export-files/google_scanning/marc-files")

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
