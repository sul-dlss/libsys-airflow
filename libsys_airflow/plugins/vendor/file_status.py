from datetime import datetime

from sqlalchemy.orm import Session

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from libsys_airflow.plugins.vendor.models import VendorInterface, VendorFile, FileStatus


# You would expect to find this in models.py. However, when running an upgrade, models.py is used
# in a context in which Airflow library is not available.
def record_status_from_context(context: Context, status: FileStatus):
    """
    Update the status of a VendorFile specified in the DAG context.
    """
    vendor_uuid = context["params"]["vendor_uuid"]
    vendor_interface_uuid = context["params"]["vendor_interface_uuid"]
    filename = context["params"]["filename"]

    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        vendor_interface = VendorInterface.load_with_vendor(
            vendor_uuid, vendor_interface_uuid, session
        )
        vendor_file = VendorFile.load_with_vendor_interface(
            vendor_interface, filename, session
        )
        vendor_file.status = status
        now = datetime.utcnow()
        vendor_file.updated = now
        if status is FileStatus.loaded:
            vendor_file.loaded_timestamp = now
            vendor_file.loaded_history = vendor_file.loaded_history + [now.isoformat()]
        session.commit()
