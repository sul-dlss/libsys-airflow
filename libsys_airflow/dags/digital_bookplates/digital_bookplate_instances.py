from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.models.param import Param

from libsys_airflow.plugins.digital_bookplates.bookplates import (
    bookplate_fund_ids,
    launch_add_979_fields_task,
)

from libsys_airflow.plugins.folio.invoices import (
    invoices_paid_within_date_range,
    paid_invoice_lines_task,
    paid_po_lines_from_invoice_lines,
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
    schedule=None,
    start_date=datetime(2024, 9, 9),
    catchup=False,
    tags=["digital bookplates"],
    params={
        "from_date": Param(
            f"{(datetime.now() - timedelta(8)).strftime('%Y-%m-%d')}",
            format="date",
            type="string",
            description="The earliest date to select paid invoices from FOLIO.",
        ),
        "to_date": Param(
            f"{(datetime.now()).strftime('%Y-%m-%d')}",
            format="date",
            type="string",
            description="The latest date to select paid invoices from FOLIO.",
        ),
    },
)
def digital_bookplate_instances():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    retrieve_paid_invoices = invoices_paid_within_date_range()

    retrieve_bookplate_fund_ids = bookplate_fund_ids()

    retrieve_paid_invoice_lines = paid_invoice_lines_task(
        retrieve_paid_invoices, retrieve_bookplate_fund_ids
    )

    retrieve_paid_po_lines = paid_po_lines_from_invoice_lines(
        retrieve_paid_invoice_lines
    )

    retrieve_instances = instances_from_po_lines(
        retrieve_paid_po_lines, retrieve_bookplate_fund_ids
    )

    launch_add_tag_dag = launch_add_979_fields_task(instances=retrieve_instances)

    start >> retrieve_paid_invoices
    launch_add_tag_dag >> end


digital_bookplate_instances()
