"""Takes 50k+ Authority or Bib MARC21 records and generates 1 or more batches of 50k."""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from libsys_airflow.plugins.authority_control.helpers import (
    create_batches,
    trigger_load_record_dag,
)


@dag(
    schedule=None,
    start_date=datetime(2025, 2, 13),
    catchup=False,
    tags=["authorities", "folio"],
)
def batch_marc21_records(*args, **kwargs):
    """
    DAG triggers one or more load_marc_file DAG runs in 50k batches from a 50k+
    record file.
    """

    @task
    def setup_dag(*args, **kwargs):
        context = get_current_context()
        params = context.get("params", {})
        file_path = params.get("file")
        if file_path is None:
            raise ValueError("File path is required")
        profile_name = params.get("profile")
        if profile_name is None:
            raise ValueError("Profile name is required")
        return {"file_path": file_path, "profile_name": profile_name}

    @task
    def batch_records(file_path: str, ti):
        return create_batches(file_path)

    @task
    def trigger_dag(*args, **kwargs):
        file_path = kwargs["file"]
        ti = kwargs["ti"]
        profile_name = ti.xcom_pull(task_ids="setup_dag", key="profile_name")
        context = get_current_context()
        dag_run = trigger_load_record_dag(file_path, profile_name)
        dag_run.execute(context)

    setup = setup_dag()
    batches = batch_records(setup["file_path"])
    trigger_dag.expand(batch=batches)


batch_marc21_records()
