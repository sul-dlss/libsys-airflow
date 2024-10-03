from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.digital_bookplates.bookplates import (
    bookplate_fund_po_lines,
    launch_add_979_fields_task,
)

from libsys_airflow.plugins.folio.invoices import (
    invoices_paid_within_date_range,
    invoice_lines_from_invoices_task,
    invoice_lines_funds_polines,
)

from libsys_airflow.plugins.folio.orders import (
    instances_from_po_lines,
)

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


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

    retrieve_paid_invoice_lines = invoice_lines_from_invoices_task(
        retrieve_paid_invoices
    )

    retrieve_invoice_line_datastruct = invoice_lines_funds_polines(
        retrieve_paid_invoice_lines
    )

    retrieve_bookplate_fund_po_lines = bookplate_fund_po_lines(
        retrieve_invoice_line_datastruct
    )

    retrieve_instances = instances_from_po_lines(
        retrieve_bookplate_fund_po_lines
    )

    launch_add_tag_dag = launch_add_979_fields_task(instances=retrieve_instances)

    start >> retrieve_paid_invoices
    launch_add_tag_dag >> end


digital_bookplate_instances()
