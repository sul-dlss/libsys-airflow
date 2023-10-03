import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task_group
from airflow.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.orafin.tasks import (
    email_errors_task,
    extract_rows_task,
    retrieve_invoice_task,
    retrieve_report_task,
    retrieve_voucher_task,
    update_invoices_task,
    update_vouchers_task,
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
    finish_updates = EmptyOperator(task_id="end", trigger_rule="all_done")

    retrieve_payment_csv = retrieve_report_task("sftp-orafin")

    report_rows = extract_rows_task(retrieve_payment_csv)

    invoices = retrieve_invoice_task.expand(row=report_rows)

    update_folio.expand(record=invoices) >> email_errors_task() >> finish_updates

