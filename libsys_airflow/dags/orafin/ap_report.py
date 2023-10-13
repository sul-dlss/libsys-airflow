import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.orafin.tasks import (
    email_errors_task,
    extract_rows_task,
    filter_files_task,
    get_new_reports_task,
    retrieve_invoice_task,
    retrieve_voucher_task,
    update_invoices_task,
    update_vouchers_task,
)

from libsys_airflow.plugins.orafin.ap_reports import (
    find_reports,
    retrieve_reports
)

logger = logging.getLogger(__name__)

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@task_group(group_id="update-folio")
def update_folio(record):
    invoice_id = update_invoices_task(invoice=record)
    voucher = retrieve_voucher_task(invoice_id)
    update_vouchers_task(voucher=voucher)


with DAG(
    "ap_payment_report",
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        cron="00 18 * * 2,4", timezone="America/Los_Angeles"
    ),
    start_date=datetime(2023, 9, 19),
    catchup=False,
    tags=["folio", "orafin"],
) as dag:
    start = EmptyOperator(task_id="start")

    finish_updates = EmptyOperator(task_id="end")

    ap_reports = filter_files_task()

    report_rows = extract_rows_task()

    new_reports = get_new_reports_task()

    retrieve_new_reports = retrieve_reports().expand(env=new_reports)

    start >> find_reports() >> ap_reports >> new_reports
    
    retrieve_new_reports >> report_rows

    invoices = retrieve_invoice_task.expand(row=report_rows)

    update_folio.expand(record=invoices) >> email_errors_task() >> finish_updates

