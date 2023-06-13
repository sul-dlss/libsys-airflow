import logging

from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import PokeReturnValue

from sqlalchemy.orm import Session

from libsys_airflow.plugins.folio.folio_client import FolioClient
from libsys_airflow.plugins.vendor.models import FileStatus, VendorFile


logger = logging.getLogger(__name__)


def _folio_client():
    return FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )


# Run the task every five minutes for up to one day
@task.sensor(poke_interval=60 * 5, timeout=60 * 60 * 24, mode="reschedule")
def file_loaded_sensor_task(
    vendor_interface_uuid: str, filename: str, upload_definition_id: str
) -> PokeReturnValue:
    return file_loaded_sensor(vendor_interface_uuid, filename, upload_definition_id)


def file_loaded_sensor(
    vendor_interface_uuid: str,
    filename: str,
    upload_definition_id: str,
    client=None,
) -> PokeReturnValue:
    folio_client = client or _folio_client()
    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        vendor_file = VendorFile.load(vendor_interface_uuid, filename, session)
        if (
            vendor_file is None
            or vendor_file.status != FileStatus.loaded
            or vendor_file.folio_job_execution_uuid is None
        ):
            logger.info(
                f"Skipping sending email since file ('{vendor_interface_uuid} - {filename}') has not been loaded."
            )
            return PokeReturnValue(is_done=False)

    upload_definition = folio_client.get(
        f"/data-import/uploadDefinitions/{upload_definition_id}"
    )

    if upload_definition["status"] in ["COMPLETED", "ERROR"]:
        return PokeReturnValue(is_done=True)

    return PokeReturnValue(is_done=False)
