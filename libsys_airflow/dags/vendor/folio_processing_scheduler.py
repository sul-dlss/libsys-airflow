from datetime import datetime, timezone
import logging

from airflow.sdk import task, DAG

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import Session

from libsys_airflow.plugins.vendor.models import VendorFile, FileStatus
from libsys_airflow.plugins.shared.utils import execution_date

logger = logging.getLogger(__name__)

# Run with:
# docker exec -it libsys-airflow-airflow-worker-1 airflow dags trigger folio_processing_scheduler

with DAG(
    dag_id="folio_processing_scheduler",
    default_args={},
    schedule="@hourly",
    catchup=False,
    start_date=datetime(2023, 1, 1),
    tags=["vma"],
    params={},
) as dag:

    @task
    def generate_dag_run_kwargs() -> list[dict]:
        """
        Generate a DAG conf for each VendorFile that needs to be loaded.
        """
        confs = []

        pg_hook = PostgresHook("vendor_loads")
        with Session(pg_hook.get_sqlalchemy_engine()) as session:
            # get up to 1000 files that need to be loaded
            # as a practical matter should we limit this further?
            vendor_files = VendorFile.ready_for_data_processing(session)
            if len(vendor_files) == 0:
                return []

            for vendor_file in vendor_files:
                vendor_interface = vendor_file.vendor_interface
                vendor = vendor_interface.vendor

                logical_date = execution_date()
                dag_run_id = f"manual__{logical_date}-{vendor_file.vendor_filename}"

                vendor_file.dag_run_id = dag_run_id
                vendor_file.dag_execution_date = execution_date
                vendor_file.updated = datetime.now(timezone.utc).isoformat()
                vendor_file.status = FileStatus.loading

                logger.info(
                    f"updated vendor_file {vendor_file}: dag_run_id={dag_run_id} logical_date={logical_date}"
                )

                confs.append(
                    {
                        "trigger_dag_id": vendor_interface.processing_dag
                        or "default_data_processor",
                        "trigger_run_id": dag_run_id,
                        "logical_date": logical_date,
                        "conf": {
                            "vendor_uuid": vendor.folio_organization_uuid,
                            "vendor_interface_uuid": vendor_interface.folio_interface_uuid,
                            "dataload_profile_uuid": vendor_interface.folio_data_import_profile_uuid,
                            "filename": vendor_file.vendor_filename,
                        },
                    }
                )

            session.commit()

        return confs

    dag_run_kwargs = generate_dag_run_kwargs()

    TriggerDagRunOperator.partial(task_id="process_file").expand_kwargs(dag_run_kwargs)
