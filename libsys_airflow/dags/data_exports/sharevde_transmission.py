import logging
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    connection_details_task,
    gather_files_task,
    transmit_data_task,
    archive_transmitted_data_task,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    default_args=default_args,
    schedule=timedelta(days=int(Variable.get("schedule_sharevde_days", 1))),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data export"],
)
def send_sharevde_records():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    gather_files = gather_files_task()

    connection_details = connection_details_task()

    transmit_data = transmit_data_task(connection_details)

    archive_data = archive_transmitted_data_task()

    start >> gather_files >> transmit_data >> archive_data >> end


send_sharevde_records()
