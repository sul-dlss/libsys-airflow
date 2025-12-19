"""Takes csv of 001 values, finds authority records, deletes and email's report"""

import logging

from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context

from libsys_airflow.plugins.authority_control.email import email_deletes_report
from libsys_airflow.plugins.authority_control.helpers import (
    archive_csv_files,
    batch_csv,
    clean_csv_file,
    delete_authorities,
    find_authority_by_001,
)

logger = logging.getLogger(__name__)


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
        updated_csv_file = kwargs.get("file")
        batches = batch_csv(file=updated_csv_file)
        return batches

    @task_group(group_id="retrieve-delete-group")
    def retrieve_and_delete_auth_records(**kwargs):

        @task(multiple_outputs=True)
        def retrieve_authority_records(**kwargs):
            csv_batch_file = kwargs.get("file")
            results = find_authority_by_001(file=csv_batch_file)
            return results

        @task
        def delete_authority_records(**kwargs):
            results = kwargs["results"]
            delete_uuids = results.get("deletes")
            results = delete_authorities(deletes=delete_uuids)
            return results

        find_results = retrieve_authority_records(file=kwargs.get("batch"))
        delete_authority_records(results=find_results)

    @task
    def move_csv_files(**kwargs):
        task_instance = kwargs["ti"]
        all_csv_files = []
        original_csv = task_instance.xcom_pull(task_ids="setup_dag", key="file")
        all_csv_files.append(original_csv)
        updated_csv = task_instance.xcom_pull(task_ids="read_csv_parse_001s")
        all_csv_files.append(updated_csv)
        batch_csvs = task_instance.xcom_pull(task_ids="batch_001s")
        all_csv_files.extend(batch_csvs)
        archive_csv_files(csv_files=all_csv_files)

    @task
    def email_report(**kwargs):
        task_instance = kwargs["ti"]
        retrieve_results = task_instance.xcom_pull(
            task_ids="retrieve-delete-group.retrieve_authority_records",
            map_indexes=None,
        )
        missing, multiples, errors = [], [], []
        for row in retrieve_results:
            missing.extend(row['missing'])
            multiples.extend(row['multiples'])
            errors.extend(row['errors'])

        deleted_results = task_instance.xcom_pull(
            task_ids="retrieve-delete-group.delete_authority_records", map_indexes=None
        )
        deleted = 0
        for row in deleted_results:
            deleted += row['deleted']
            errors.extend(row['errors'])

        email = task_instance.xcom_pull(task_ids="setup_dag", key="email")

        email_deletes_report(
            missing=missing,
            multiples=multiples,
            email=email,
            deleted=int(deleted),
            errors=errors,
            **kwargs,
        )

    updated_csv = read_csv_parse_001s()
    batches_001s = batch_001s(file=updated_csv)

    setup_dag() >> updated_csv

    retrieve_and_delete_auth_records.expand(batch=batches_001s) >> [
        move_csv_files(),
        email_report(),
    ]


delete_authority_records()
