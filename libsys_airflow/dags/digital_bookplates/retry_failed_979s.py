from datetime import datetime

from airflow.sdk import dag, get_current_context, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.trigger import CronTriggerTimetable

from libsys_airflow.plugins.digital_bookplates.dag_979_retries import (
    failed_979_dags,
    run_ids,
    clear_failed_add_marc_tags_to_record,
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

    @task
    def find_failed_979_dags():
        context = get_current_context()
        bash_operator = failed_979_dags()
        return bash_operator.execute(context)

    failed_dag_runs = find_failed_979_dags()

    dag_run_ids = run_ids(failed_dag_runs)

    rerun_failed_979_dags = clear_failed_add_marc_tags_to_record()

    start >> rerun_failed_979_dags >> poll_for_979s_dags(dag_run_ids) >> end


retry_failed_979s()
