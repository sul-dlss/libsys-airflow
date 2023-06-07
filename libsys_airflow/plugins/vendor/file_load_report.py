import json
import logging
import requests

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
def report_when_file_loaded_task(
    vendor_interface_uuid: str, filename: str
) -> PokeReturnValue:
    return report_when_file_loaded(vendor_interface_uuid, filename)


def report_when_file_loaded(
    vendor_interface_uuid: str,
    filename: str,
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

    try:
        job_summary = folio_client.get(
            f"/metadata-provider/jobSummary/{vendor_file.folio_job_execution_uuid}"
        )
    except requests.exceptions.HTTPError as error:
        if error.response.status_code == 404:
            logger.info(
                f"Skipping sending email since file ('{vendor_interface_uuid} - {filename}') is in the process of being loaded."
            )
            return PokeReturnValue(is_done=False)
        else:
            logger.info(
                f"Skipping sending email since file ('{vendor_interface_uuid} - {filename}') load endpoint is erroring ({error.response.status_code})."
            )
            raise

    source_record_stats = job_summary.get("sourceRecordSummary", {})
    instance_stats = job_summary.get("instanceSummary", {})
    records_touched = (
        source_record_stats.get("totalCreatedEntities", 0)
        + source_record_stats.get("totalUpdatedEntities", 0)
        + source_record_stats.get("totalDiscardedEntities", 0)
        + source_record_stats.get("totalErrors", 0)
        + instance_stats.get("totalCreatedEntities", 0)
        + instance_stats.get("totalUpdatedEntities", 0)
        + instance_stats.get("totalDiscardedEntities", 0)
        + instance_stats.get("totalErrors", 0)
    )
    if records_touched == 0:
        logger.info(
            f"Skipping sending email since file ('{vendor_interface_uuid} - {filename}') is in the process of being loaded."
        )
        return PokeReturnValue(is_done=False)

    # NOTE: We must return the relevant reporting values as a JSON string since
    #       we can't return a dict. Why can't we? Because sensor tasks
    #       (apparently) do not support `multiple_outputs`.
    return PokeReturnValue(
        is_done=True,
        xcom_value=json.dumps(
            {
                "folio_job_execution_uuid": vendor_file.folio_job_execution_uuid,
                "srs_stats": source_record_stats,
                "instance_stats": instance_stats,
            }
        ),
    )
