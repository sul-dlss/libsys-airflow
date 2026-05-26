"""Takes csv of 001 values, finds authority records, deletes and email's report"""

import logging

from datetime import datetime
from airflow.sdk import dag, task, get_current_context

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
    def read_csv_parse_001s(csv_file):
        update_csv_file_path = clean_csv_file(file=csv_file)
        return update_csv_file_path

    @task
    def batch_001s(updated_csv_file):
        batches = batch_csv(file=updated_csv_file)
        return batches

    @task(multiple_outputs=True)
    def retrieve_authority_records(csv_batch_file):
        results = find_authority_by_001(file=csv_batch_file)
        return results

    @task
    def delete_authority_records(results):
        delete_uuids = results.get("deletes")
        results = delete_authorities(deletes=delete_uuids)
        return results

    @task
    def move_csv_files(original_csv, updated_csv, batch_csvs, trigger):
        all_csv_files = [original_csv, updated_csv]
        all_csv_files.extend(batch_csvs)
        archive_csv_files(csv_files=all_csv_files)

    @task
    def email_report(retrieve_results, deleted_results, email, **context):
        """
        retrieve_results and deleted_results are automatically lists
        containing outputs from all mapped task instances
        """
        missing, multiples, errors = [], [], []
        for row in retrieve_results:
            missing.extend(row.get("missing", []))
            multiples.extend(row.get("multiples", []))
            errors.extend(row.get("errors", []))

        deleted = 0
        for row in deleted_results:
            deleted += row.get("deleted", 0)
            errors.extend(row.get("errors", []))

        email_deletes_report(
            missing=missing,
            multiples=multiples,
            email=email,
            deleted=int(deleted),
            errors=errors,
            **context,
        )

    # DAG flow
    setup = setup_dag()

    updated_csv = read_csv_parse_001s(csv_file=setup["file"])
    batches_001s = batch_001s(updated_csv_file=updated_csv)

    # Mapped tasks - these expand based on the batches
    retrieve_results = retrieve_authority_records.expand(csv_batch_file=batches_001s)
    delete_results = delete_authority_records.expand(results=retrieve_results)

    # Downstream tasks that consume all mapped results
    move_csv_files(
        original_csv=setup["file"],
        updated_csv=updated_csv,
        batch_csvs=batches_001s,
        trigger=delete_results,
    )

    email_report(
        retrieve_results=retrieve_results,
        deleted_results=delete_results,
        email=setup["email"],
    )


delete_authority_records()
