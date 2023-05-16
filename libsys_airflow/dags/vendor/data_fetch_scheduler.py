from datetime import datetime, timedelta
import logging

from sqlalchemy.orm import Session
from sqlalchemy import true

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


from libsys_airflow.plugins.vendor.models import VendorInterface

logger = logging.getLogger(__name__)

default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_fetcher_scheduler",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    start_date=datetime(2023, 1, 1),
) as dag:

    @task
    def generate_dag_run_conf() -> list[dict]:
        """
        Generate a DAG conf for each active vendor.
        """
        pg_hook = PostgresHook("vendor_loads")
        confs = []
        with Session(pg_hook.get_sqlalchemy_engine()) as session:
            for vendor_interface in (
                session.query(VendorInterface)
                .filter(VendorInterface.active == true())
                .join(VendorInterface.vendor)
            ):
                confs.append(
                    {
                        "vendor_code": vendor_interface.vendor.vendor_code_from_folio,
                        "vendor_uuid": vendor_interface.vendor.folio_organization_uuid,
                        "vendor_interface_uuid": vendor_interface.folio_interface_uuid,
                        "dataload_profile_uuid": vendor_interface.folio_data_import_profile_uuid,
                        "remote_path": vendor_interface.remote_path or "",
                        "filename_regex": vendor_interface.file_pattern,
                        "processing_delay": vendor_interface.processing_delay_in_days
                        or 0,
                        "processing_dag": vendor_interface.processing_dag
                        or "default_data_processor",
                    }
                )
        return confs

    dag_run_confs = generate_dag_run_conf()
    TriggerDagRunOperator.partial(
        task_id="fetch_data", trigger_dag_id="data_fetcher"
    ).expand(conf=dag_run_confs)
