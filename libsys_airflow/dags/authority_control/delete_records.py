"""Takes csv of 001 values, finds authority records, deletes and email's report"""

from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context

from libsys_airflow.plugins.authority_control.helpers import (
    clean_csv_file,
)


@dag(
    schedule=None,
    start_date=datetime(2025, 12, 15),
    catchup=False,
    tags=["authorities", "folio"],
)
def delete_authority_records(*args, **kwargs):
    """
    DAG uses incoming csv of 001s, finds corresponding authority record, and
    attempts to delete the record. If there duplicates or if an authority record
    cannot be found, or is successful, emails report.
    """

    @task(multiple_outputs=True)
    def setup_dag(*args, **kwargs):
        context = get_current_context()
        params = context.get("params", {})
        csv_file = params["kwargs"].get("file")
        if csv_file is None:
            raise ValueError("CSV file of 001 values is required")
        email_addr = params["kwargs"].get("email")
        return {"file": csv_file, "email": email_addr}

    @task
    def read_csv_parse_001s(**kwargs):
        task_instance = kwargs["ti"]
        csv_file = task_instance.xcom_pull(task_ids="setup_dag", key="file")
        update_csv_file_path = clean_csv_file(file=csv_file)
        return update_csv_file_path

    @task
    def batch_001s(**kwargs):
        pass

    @task_group(group_id="retrieve-delete-group")
    def retrieve_and_delete_auth_records(**kwargs):

        @task
        def retrieve_authority_records(**kwargs):
            pass

        @task
        def delete_authority_records(**kwargs):
            pass

        retrieve_authority_records() >> delete_authority_records()

    @task
    def email_report(**kwargs):
        pass

    batches_001s = batch_001s()

    setup_dag() >> read_csv_parse_001s() >> batches_001s

    retrieve_and_delete_auth_records.expand(batch=batches_001s) >> email_report()


delete_authority_records()
