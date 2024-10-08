from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
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

    @task
    def get_funds() -> list:
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        return params.get("funds", [])

    funds = get_funds()

    retrieve_paid_invoices = invoices_paid_within_date_range()

    retrieve_paid_invoice_lines = invoice_lines_from_invoices(retrieve_paid_invoices)

    filter_bookplate_fund_po_lines = bookplate_funds_polines(
        retrieve_paid_invoice_lines, funds
    )

    retrieve_instances = instances_from_po_lines(filter_bookplate_fund_po_lines)

    launch_add_tag_dag = launch_add_979_fields_task(instances=retrieve_instances)

    start >> [funds, retrieve_paid_invoices]
    launch_add_tag_dag >> end


digital_bookplate_instances()
