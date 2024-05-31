import pathlib

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from airflow.decorators import task

from libsys_airflow.plugins.vendor.purge import remove_files


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "data_export_purge_archived_files",
    default_args=default_args,
    start_date=datetime(2024, 5, 30),
    schedule=timedelta(days=1),
    catchup=False,
    tags=["data export"],
) as dag:

    @task
    def gather_files_task(**kwargs) -> list[pathlib.Path]:
        from libsys_airflow.plugins.vendor.purge import find_files

        airflow = kwargs.get("airflow", "/opt/airflow")
        _directory = pathlib.Path((airflow) / "data-export-files/*/transmitted/")
        return find_files(downloads_directory=_directory)

    start = EmptyOperator(task_id='start_removing_archived')

    finish = EmptyOperator(task_id='finish_removing_archived')

    gathered_files = gather_files_task()

    remove_archived_files = remove_files(target_files=gathered_files)  # type: ignore

    start >> gathered_files >> remove_archived_files >> finish
