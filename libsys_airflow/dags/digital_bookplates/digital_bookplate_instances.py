from datetime import datetime, timedelta

from airflow.decorators import dag, task_group
from airflow.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable


from libsys_airflow.plugins.digital_bookplates.bookplates import (
    bookplate_funds_polines,
    launch_add_979_fields_task,
)

from libsys_airflow.plugins.folio.invoices import (
    invoices_paid_within_date_range,
    invoice_lines_from_invoices,
)

from libsys_airflow.plugins.folio.orders import (
    instances_from_po_lines,
)

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


@task_group(group_id="process-invoice-lines")
def process_invoice_lines_group(invoice_id: str):
    paid_invoice_lines = invoice_lines_from_invoices(invoice_id)
    paid_bookplate_polines = bookplate_funds_polines(invoice_lines=paid_invoice_lines)
    return instances_from_po_lines(po_lines_funds=paid_bookplate_polines)


@dag(
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        cron="0 2 * * WED", timezone="America/Los_Angeles"
    ),
    start_date=datetime(2023, 8, 28),
    catchup=False,
    tags=["digital bookplates"],
)
def digital_bookplate_instances():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    retrieve_paid_invoices = invoices_paid_within_date_range()

    retrieve_instances = process_invoice_lines_group.expand(
        invoice_id=retrieve_paid_invoices
    )

    launch_add_tag_dag = launch_add_979_fields_task(instances=retrieve_instances)

    start >> retrieve_paid_invoices
    launch_add_tag_dag >> end


digital_bookplate_instances()
