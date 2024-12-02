from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.digital_bookplates.dag_979_retries import (
    failed_979_dags,
    run_failed_979_dags,
)


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}


@dag(
    default_args=default_args,
    start_date=datetime(2024, 10, 15),
    schedule=CronDataIntervalTimetable(
        cron="45 20 7 * *", timezone="America/Los_Angeles"
    ),
    catchup=False,
    tags=["digital bookplates"],
)
def retry_failed_979s():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    find_failed_979_dags = failed_979_dags()

    rerun_failed_979_dags = run_failed_979_dags(dags=find_failed_979_dags)

    start >> find_failed_979_dags >> rerun_failed_979_dags >> end


retry_failed_979s()
