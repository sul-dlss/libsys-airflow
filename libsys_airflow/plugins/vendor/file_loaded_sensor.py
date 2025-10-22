import logging

import httpx

from airflow.sdk import task, PokeReturnValue, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy.orm import Session

from folioclient import FolioClient
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
    vendor_interface_uuid: str, filename: str, job_execution_id: str
) -> PokeReturnValue:
    return file_loaded_sensor(vendor_interface_uuid, filename, job_execution_id)


def file_loaded_sensor(
    vendor_interface_uuid: str,
    filename: str,
    job_execution_id: str,
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
                f"File ('{vendor_interface_uuid} - {filename}') has not been loaded yet."
            )
            return PokeReturnValue(is_done=False)

    try:
        job_execution = folio_client.folio_get(
            f"/change-manager/jobExecutions/{job_execution_id}"
        )
    except httpx.HTTPError as error:
        if error.response.status_code == 404:  # type: ignore
            logger.info(
                f"Processing job for file ('{vendor_interface_uuid} - {filename}') has not started yet."
            )
            return PokeReturnValue(is_done=False)
        else:
            raise

    if job_execution["status"] in ["COMMITTED", "ERROR"]:
        return PokeReturnValue(is_done=True)

    logger.info(
        f"File ('{vendor_interface_uuid} - {filename}') has not been processed yet."
    )
    return PokeReturnValue(is_done=False)
