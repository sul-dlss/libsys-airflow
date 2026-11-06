from pathlib import Path

STAGED_FILES_BASE = Path("/opt/airflow/data-export-files/google_scanning/staged")

# Sibling of STAGED_FILES_BASE. The on_campus_shipment DAG moves each
# shipped cart's staged file + status.json to carts-waiting/{stem}/{cart}/
# until the send_google_scanning_files DAG uploads that shipment to Drive.
CARTS_WAITING_BASE = Path(
    "/opt/airflow/data-export-files/google_scanning/carts-waiting"
)
SHIPMENT_FILENAME = "shipment.json"

# Kept indefinitely for auditing (unlike transmitted/, which is purged).
# Once a shipment is sent, each cart gets archived/{cart}/{timestamp}/
# with its barcode file, status.json, and copies of the shipment's
# MARCXML and manifest. SAL3 shipments have no carts and are copied to
# archived/sal3/{stem}/ instead, so "sal3" can't be used as a cart name.
ARCHIVED_FILES_BASE = Path("/opt/airflow/data-export-files/google_scanning/archived")
SAL3_ARCHIVE_NAME = "sal3"

# The shipment's MARCXML is written under "new" and its manifest under
# "manifests" (see manifest.py::generate_manifest), where they wait for
# the send_google_scanning_files DAG to upload them.
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

# Valid values for a waiting shipment's shipment.json "status" field.
# The send DAG only uploads "ready" shipments. queue_shipment marks a
# shipment "building" only while it moves carts and files into place.
SHIPMENT_BUILDING = "building"
SHIPMENT_READY = "ready"

# The on_campus_shipment DAG writes a shipment's MARCXML, manifest and
# instance ids under "{stem}-building" until queue_shipment renames them
# into place, so a failed build leaves the earlier build's files intact.
BUILDING_SUFFIX = "-building"

# Used only by when status.json is missing or unreadable not written by processing DAGs.
STATUS_UNKNOWN = "unknown"
