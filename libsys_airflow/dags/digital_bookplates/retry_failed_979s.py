from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.trigger import CronTriggerTimetable

from libsys_airflow.plugins.digital_bookplates.dag_979_retries import (
    failed_979_dags,
    clear_dag_runs,
    poll_for_979s_dags,
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
    schedule=CronTriggerTimetable(cron="45 20 7 * *", timezone="America/Los_Angeles"),
    catchup=False,
    tags=["digital bookplates"],
)
def retry_failed_979s():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    failed_dag_runs = failed_979_dags()

    rerun_failed_979_dags = clear_dag_runs(dag_runs=failed_dag_runs)

    start >> rerun_failed_979_dags >> poll_for_979s_dags(rerun_failed_979_dags) >> end


retry_failed_979s()
