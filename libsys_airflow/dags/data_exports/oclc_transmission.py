import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task, task_group
from airflow.sdk import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

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
    holdings_unset_errors_task,
    new_oclc_marc_errors_task,
)

from libsys_airflow.plugins.data_exports.email import (
    generate_holdings_errors_emails,
    generate_oclc_new_marc_errors_email,
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
    tags=["data export", "oclc"],
    schedule=CronDataIntervalTimetable(
        cron=Variable.get("transmit_oclc", "30 3 * * *"),
        timezone="America/Los_Angeles",
    ),
    start_date=datetime(2025, 1, 14),
    catchup=False,
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
        failed_matches=matched_records["failures"],
    )

    new_records = new_to_oclc_task(
        connection_details=connections, new_records=filtered_new_records
    )

    new_records >> set_holdings_for_records

    filtered_errors = filter_failures_task(
        update=set_holdings_for_records['failures'],
        delete=deleted_records['failures'],
        match=matched_records['failures'],
        new=new_records['failures'],
    )

    @task
    def holdings_errors_email_task(reports: dict):
        generate_holdings_errors_emails(reports)

    @task
    def marc_errors_email_task(reports: dict):
        generate_oclc_new_marc_errors_email(reports)

    @task_group(group_id="reports-email")
    def reports_email():
        holdings_set_reports = holdings_set_errors_task(failures=filtered_errors)

        holdings_set_match_reports = holdings_set_errors_task(
            failures=filtered_errors, match=True
        )

        holdings_unset_reports = holdings_unset_errors_task(failures=filtered_errors)

        new_oclc_marc_errors_reports = new_oclc_marc_errors_task(
            failures=filtered_errors
        )

        end_emails = EmptyOperator(task_id="end-reports-emails")

        [
            holdings_errors_email_task(holdings_set_reports),
            holdings_errors_email_task(holdings_set_match_reports),
            holdings_errors_email_task(holdings_unset_reports),
            marc_errors_email_task(new_oclc_marc_errors_reports),
        ] >> end_emails

    archive_files = consolidate_oclc_archive_files(gather_files)

    archive_data = archive_transmitted_data_task(archive_files)

    set_holdings_for_records >> archive_data

    start >> gather_files >> filtered_errors

    [reports_email(), archive_data] >> end


send_oclc_records()
