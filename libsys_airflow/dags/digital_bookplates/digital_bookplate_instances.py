from datetime import datetime, timedelta

from airflow.decorators import dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable


from libsys_airflow.plugins.digital_bookplates.bookplates import (
    bookplate_funds_polines,
    instances_from_po_lines,
    trigger_digital_bookplate_979_task,
)

from libsys_airflow.plugins.folio.invoices import (
    invoices_paid_within_date_range,
    invoice_lines_from_invoices,
    invoice_lines_paid_on_fund,
    date_range_or_funds_path,
)


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


@task_group(group_id="process-invoices")
def process_date_range_group(invoice_id: str):
    """
    Input: a single invoice uuid from the mapped task list
    """
    paid_invoice_lines = invoice_lines_from_invoices(invoice_id)
    paid_bookplate_polines = bookplate_funds_polines(invoice_lines=paid_invoice_lines)
    return instances_from_po_lines(
        po_lines_funds=paid_bookplate_polines
    )  # -> launch_add_979_fields_task


@task_group(group_id="process-new-funds")
def process_new_funds_group(invoice_line: dict):
    """
    See https://s3.amazonaws.com/foliodocs/api/mod-invoice-storage/p/invoice.html#invoice_storage_invoice_lines_get
    Input: an single invoice line dictionary, wrapped in a list:
    """
    paid_bookplate_polines = bookplate_funds_polines(invoice_lines=[invoice_line])
    return instances_from_po_lines(
        po_lines_funds=paid_bookplate_polines
    )  # -> launch_add_979_fields_task


@dag(
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        cron="0 2 * * WED", timezone="America/Los_Angeles"
    ),
    start_date=datetime(2023, 8, 28),
    catchup=False,
    max_active_runs=10,
    tags=["digital bookplates"],
)
def digital_bookplate_instances():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    choose_branch = date_range_or_funds_path()

    # Date range branch
    retrieve_paid_invoices = invoices_paid_within_date_range()
    retrieve_instances = process_date_range_group.expand(
        invoice_id=retrieve_paid_invoices
    )
    launch_add_tag = trigger_digital_bookplate_979_task(instances=retrieve_instances)

    # New funds branch
    paid_invoice_lines_new_fund = invoice_lines_paid_on_fund()
    retrieve_instances_new_fund = process_new_funds_group.expand(
        invoice_line=paid_invoice_lines_new_fund
    )
    launch_add_tag_new_fund = trigger_digital_bookplate_979_task(
        instances=retrieve_instances_new_fund
    )

    start >> choose_branch >> [retrieve_paid_invoices, paid_invoice_lines_new_fund]
    retrieve_paid_invoices >> launch_add_tag >> end
    paid_invoice_lines_new_fund >> launch_add_tag_new_fund >> end


digital_bookplate_instances()
