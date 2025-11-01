from datetime import datetime, timedelta

from airflow.sdk import DAG, task_group
from airflow.providers.standard.operators.empty import EmptyOperator

from libsys_airflow.plugins.orafin.tasks import (
    email_errors_task,
    email_invoice_errors_task,
    email_paid_task,
    extract_rows_task,
    init_processing_task,
    retrieve_invoice_task,
    retrieve_voucher_task,
    update_email_branch,
    update_invoices_task,
    update_vouchers_task,
)


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
    voucher_result = retrieve_voucher_task()
    update_email_branch(invoice_id) >> [voucher_result, email_invoice_errors_task()]

    voucher_result >> update_vouchers_task()


@task_group(group_id="email-group")
def email_group():
    email_errors_task()
    email_paid_task()


with DAG(
    "ap_payment_report",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 9, 19),
    catchup=False,
    tags=["folio", "orafin"],
    max_active_runs=2,
) as dag:
    start = EmptyOperator(task_id="start")

    finish_updates = EmptyOperator(task_id="end")

    report_path = init_processing_task()

    report_rows = extract_rows_task(report_path=report_path)

    start >> report_path

    invoices = retrieve_invoice_task.expand(row=report_rows)

    update_folio.expand(record=invoices) >> email_group() >> finish_updates
