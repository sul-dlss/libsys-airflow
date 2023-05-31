import logging

from airflow.decorators import task
from airflow.models import DagRun, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.base import PokeReturnValue

from sqlalchemy.orm import Session

from libsys_airflow.plugins.folio.folio_client import FolioClient
from libsys_airflow.plugins.vendor.emails import file_loaded_email_task
from libsys_airflow.plugins.vendor.models import FileStatus, VendorFile


logger = logging.getLogger(__name__)


def _folio_client():
    return FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )


# Run the task every hour for up to five days
@task.sensor(poke_interval=3600, timeout=60 * 60 * 24 * 5, mode="reschedule")
def report_when_file_loaded_task(
    vendor_interface_uuid: str, filename: str, records_count: int
) -> PokeReturnValue:
    return report_when_file_loaded(vendor_interface_uuid, filename, records_count)


def report_when_file_loaded(
    vendor_interface_uuid: str,
    filename: str,
    records_count: int,
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
                f"Skipping sending email since file ('{vendor_interface_uuid} - {filename}') has not yet been loaded."
            )
            return PokeReturnValue(is_done=False)

        # Date and Time Load was started (using the DagRun data should be sufficient)
        dag_run = DagRun.find(run_id=vendor_file.dag_run_id)[0]
        load_time = dag_run.start_date

        job_summary = folio_client.get(
            f"/metadata_provider/job-summary/{vendor_file.folio_job_execution_uuid}"
        )

        srs_stats = job_summary["sourceRecordSummary"]
        instance_stats = job_summary["instanceSummary"]

        file_loaded_email_task(
            vendor_file.vendor_interface.vendor.vendor_code_from_folio,
            vendor_file.vendor_interface.vendor.display_name,
            vendor_file.folio_job_execution_uuid,
            filename,
            load_time,
            records_count,
            srs_stats,
            instance_stats,
        )

        return PokeReturnValue(is_done=True)
