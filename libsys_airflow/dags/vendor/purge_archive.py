from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

from libsys_airflow.plugins.shared.purge import (
    discover_task,
    remove_archives_task,
    remove_downloads_task,
    set_status_task,
)

logger = logging.getLogger(__name__)

# mypy: disable-error-code = "index, arg-type"

default_args = dict(
    {
        "owner": "folio",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)


with DAG(
    dag_id="vma_purge_archived_files",
    default_args=default_args,
    start_date=datetime(2023, 5, 9),
    schedule="0 9 * * *",  # Runs Daily at 2 am PT
    tags=["vma"],
) as dag:
    finish_task = EmptyOperator(task_id="finished-purge")

    targets = discover_task()

    delete_files = remove_downloads_task(targets["downloads"])

    vendor_interfaces = remove_archives_task(targets["archive"])

    purged_status = set_status_task(vendor_interfaces)

    delete_files >> finish_task
    purged_status >> finish_task
