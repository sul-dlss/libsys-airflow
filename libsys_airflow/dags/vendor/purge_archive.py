from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.vendor.purge import (
    discover_task,
    remove_records_task,
    set_status_task
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="purge_archived_files",
    default_args=default_args,
    start_date=datetime(2023, 5, 9),
    schedule_interval="0 9 * * *"  # Runs Daily at 2 am PT
) as dag:

    target_directories = discover_task()

    vendor_interfaces = remove_records_task(target_directories)

    set_status_task(vendor_interfaces)
