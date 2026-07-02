from datetime import datetime, timedelta

from airflow.sdk import DAG, task_group
from airflow.providers.standard.operators.empty import EmptyOperator

from libsys_airflow.plugins.orafin.emails import generate_failed_dag_email

from libsys_airflow.plugins.orafin.tasks import (
    email_paid_task,
    email_invoice_errors_task,
    email_vouchers_errors_task,
    extract_rows_task,
    init_processing_task,
    process_folio_results_task,
    retrieve_invoice_task,
    retrieve_voucher_task,
    update_invoices_task,
    update_vouchers_task,
)


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@task_group(group_id="update-folio")
def update_folio(record):
    update_invoice_result = update_invoices_task(invoice=record)  # returns dict
    voucher_result = retrieve_voucher_task(
        update_invoice_result
    )  # returns None(skipped) or voucher retrieval dict
    update_voucher_result = update_vouchers_task(
        voucher_result, update_invoice_result
    )  # returns None (skipped) or voucher update dict
    return [{"invoice": update_invoice_result, "voucher": update_voucher_result}]
    # invoice will have dict; voucher could be None or dict


@task_group(group_id="email-group")
def email_group(update_results, report_path):
    """
    update_results = {
        "already_paid_invoices": already_paid_invoices,
        "already_paid_vouchers": already_paid_vouchers,
        "failed_invoice_updates": failed_invoice_updates,
        "failed_voucher_updates": failed_voucher_updates,
        "cancelled_invoices": cancelled_invoices,
        "missing_invoices": missing_invoices,
        "missing_vouchers": missing_vouchers,
        "multiple_vouchers": multiple_vouchers,
        "successful_invoice_updates": successful_invoice_updates,
        "successful_voucher_updates": successful_voucher_updates,
    }
    """
    email_invoice_errors_task(
        update_results["missing_invoices"],
        update_results["cancelled_invoices"],
        update_results["already_paid_invoices"],
        update_results["failed_invoice_updates"],
    )
    email_paid_task(update_results["successful_invoice_updates"], report_path)
    email_vouchers_errors_task(
        update_results["missing_vouchers"],
        update_results["multiple_vouchers"],
        update_results["failed_voucher_updates"],
    )


with DAG(
    "ap_payment_report",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 9, 19),
    catchup=False,
    tags=["folio", "orafin"],
    on_failure_callback=generate_failed_dag_email,
    max_active_runs=2,
) as dag:
    start = EmptyOperator(task_id="start")

    finish_updates = EmptyOperator(task_id="end", trigger_rule="all_done")

    report_path = init_processing_task()

    report_rows = extract_rows_task(report_path=report_path)

    start >> report_path

    invoices = retrieve_invoice_task.expand(row=report_rows)

    update_results = update_folio.expand(record=invoices)

    (
        email_group(process_folio_results_task(update_results), report_path)
        >> finish_updates
    )
