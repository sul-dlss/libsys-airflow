import logging
from datetime import datetime, timedelta

from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.data_exports.email import generate_oclc_transmission_email

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    archive_transmitted_data_task,
    gather_oclc_files_task,
    transmit_data_oclc_api_task,
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


@task
def failure_email_task(failures: dict):
    for library_code, instance_uuids in failures.items():
        generate_oclc_transmission_email("failure", instance_uuids, library_code)


@task
def success_email_task(successes: dict):
    for library_code, instance_uuids in successes.items():
        generate_oclc_transmission_email("success", instance_uuids, library_code)


@dag(
    default_args=default_args,
    schedule=timedelta(days=int(Variable.get("schedule_oclc_days", 7))),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data export"],
)
def send_oclc_records():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    gather_files = gather_oclc_files_task()

    transmit_data = transmit_data_oclc_api_task(
        [
            "http-web.oclc-Business",
            "http-web.oclc-Hoover",
            "http-web.oclc-Lane",
            "http-web.oclc-Law",
            "http-web.oclc-SUL",
        ],
        gather_files,
    )

    archive_data = archive_transmitted_data_task(transmit_data['success'])
    success_email = success_email_task(transmit_data['success'])
    failed_email = failure_email_task(transmit_data['failures'])

    start >> gather_files >> transmit_data
    transmit_data >> archive_data >> [success_email, failed_email] >> end


send_oclc_records()
