import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task_group
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    archive_transmitted_data_task,
    consolidate_oclc_archive_files,
    delete_from_oclc_task,
    filter_new_marc_records_task,
    match_oclc_task,
    new_to_oclc_task,
    set_holdings_oclc_task,
    gather_oclc_files_task,
)

from libsys_airflow.plugins.data_exports.oclc_reports import (
    filter_failures_task,
    holdings_set_errors_task,
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

connections = [
    "http-web.oclc-Business",
    "http-web.oclc-Hoover",
    "http-web.oclc-Lane",
    "http-web.oclc-Law",
    "http-web.oclc-SUL",
]


@dag(
    default_args=default_args,
    schedule=timedelta(
        days=int(Variable.get("schedule_oclc_days", 7)),
        hours=int(Variable.get("schedule_oclc_hours", 13)),
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data export", "oclc"],
)
def send_oclc_records():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    gather_files = gather_oclc_files_task()

    deleted_records = delete_from_oclc_task(
        connection_details=connections, delete_records=gather_files["deletes"]
    )

    set_holdings_for_records = set_holdings_oclc_task(
        connection_details=connections, update_records=gather_files["updates"]
    )

    matched_records = match_oclc_task(
        connection_details=connections, new_records=gather_files["new"]
    )

    filtered_new_records = filter_new_marc_records_task(
        new_records=gather_files["new"],
        new_instance_uuids=matched_records["failures"],
    )

    new_records = new_to_oclc_task(
        connection_details=connections, new_records=filtered_new_records
    )

    filtered_errors = filter_failures_task(
        update=set_holdings_for_records['failures'],
        deleted=deleted_records['failures'],
        match=matched_records['failures'],
        new=new_records['failures'],
    )

    @task_group(group_id="reports-email")
    def reports_email():
        holdings_set_reports = holdings_set_errors_task(failures=filtered_errors)

        holdings_set_match_report = holdings_set_errors_task(
            failures=filtered_errors, match=True
        )

    archive_files = consolidate_oclc_archive_files(
        deleted_records["archive"],
        new_records["archive"],
        matched_records["archive"],
        set_holdings_for_records["archive"],
    )

    archive_data = archive_transmitted_data_task(archive_files)

    start >> gather_files >> filtered_errors

    [archive_data] >> end


send_oclc_records()
