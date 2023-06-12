from datetime import datetime

from sqlalchemy.orm import Session

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from libsys_airflow.plugins.vendor.models import VendorFile, FileStatus


# You would expect to find this in models.py. However, when running an upgrade, models.py is used
# in a context in which Airflow library is not available.
def record_status_from_context(context: Context, status: FileStatus):
    """
    Update the status of a VendorFile specified in the DAG context.
    """
    vendor_interface_uuid = context["params"]["vendor_interface_uuid"]
    filename = context["params"]["filename"]

    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        vendor_file = VendorFile.load(vendor_interface_uuid, filename, session)
        vendor_file.status = status
        vendor_file.updated = datetime.utcnow()
        if status is FileStatus.loaded:
            vendor_file.loaded_timestamp = datetime.utcnow()
        session.commit()
