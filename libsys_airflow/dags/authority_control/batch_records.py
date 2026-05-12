"""Takes 50k+ Authority or Bib MARC21 records and generates 1 or more batches of 50k."""

from datetime import datetime

from airflow.sdk import (
    dag,
    get_current_context,
    task,
)

from libsys_airflow.plugins.authority_control.helpers import (
    clean_up,
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

    @task(multiple_outputs=True)
    def setup_dag(*args, **kwargs):
        context = get_current_context()
        params = context.get("params", {})
        file_path = params["kwargs"].get("file")
        if file_path is None:
            raise ValueError("File path is required")
        profile_name = params["kwargs"].get("profile")
        if profile_name is None:
            raise ValueError("Profile name is required")
        return {"file_path": file_path, "profile_name": profile_name}

    @task
    def batch_records(file_path: str, ti):
        return create_batches(file_path)

    @task
    def trigger_dag(*args, **kwargs):
        file_name = kwargs["batch"]
        ti = kwargs["ti"]
        profile_name = ti.xcom_pull(task_ids="setup_dag", key="profile_name")
        context = get_current_context()
        dag_run = trigger_load_record_dag(
            f"/opt/airflow/authorities/{file_name}", profile_name
        )
        dag_run.execute(context)

    @task
    def clean_up_dag(*args, **kwargs):
        task_instance = kwargs["ti"]
        marc_file_path = task_instance.xcom_pull(task_ids="setup_dag", key="file_path")
        return clean_up(marc_file_path)

    setup = setup_dag()
    batches = batch_records(setup["file_path"])
    trigger_dag.expand(batch=batches) >> clean_up_dag()


batch_marc21_records()
