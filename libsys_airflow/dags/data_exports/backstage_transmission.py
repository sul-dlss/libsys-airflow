import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    gather_files_task,
    transmit_data_ftp_task,
    archive_transmitted_data_task,
)

from libsys_airflow.plugins.data_exports.email import (
    failed_transmission_email,
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
    schedule=CronDataIntervalTimetable(
        cron=Variable.get("transmit_backstage", "30 1 * * SAT"),
        timezone="America/Los_Angeles",
    ),
    start_date=datetime(2024, 11, 18),
    catchup=False,
    tags=["data export", "backstage"],
)
def send_backstage_records():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    gather_files = gather_files_task(vendor="backstage")

    transmit_data = transmit_data_ftp_task("backstage", gather_files)

    archive_data = archive_transmitted_data_task(transmit_data['success'])

    email_failures = failed_transmission_email(transmit_data["failures"])

    start >> gather_files >> transmit_data >> [archive_data, email_failures] >> end


send_backstage_records()
