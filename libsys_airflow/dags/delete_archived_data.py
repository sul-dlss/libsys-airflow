from datetime import datetime, timedelta
import logging
from pathlib import Path

from airflow import DAG

from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def delete_archived_data(*args, **kwargs):
    """Delete archived JSON Instances, Items, and Holdings"""
    airflow = kwargs.get("airflow", "/opt/airflow")
    airflow_path = Path(airflow)
    dag = kwargs["dag_run"]

    archive_directory = airflow_path / f"migration/iterations/{dag.run_id}/archive"

    for artifact in archive_directory.glob("*.json"):
        modified = datetime.fromtimestamp(Path(artifact).stat().st_mtime)
        yesterday = datetime.today() - timedelta(days=1)
        if modified.date() >= yesterday.date():
            try:
                artifact.unlink()
            except OSError as err:
                logger.error(f"Cannot remove {artifact}: {err}")


with DAG(
    "remove_archived_migration_files",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 3),
    catchup=False,
    tags=["remove_archived"],
) as dag:

    archive_files = PythonOperator(
        task_id="remove_archived_migration_files", python_callable=delete_archived_data
    )


archive_files
