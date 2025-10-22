import logging

from datetime import datetime, timedelta

from airflow.sdk import dag, task, task_group, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable


from libsys_airflow.plugins.digital_bookplates.bookplates import (
    bookplate_funds_polines,
    instances_from_po_lines,
    trigger_digital_bookplate_979_task,
    trigger_poll_for_979s_task,
)

from libsys_airflow.plugins.folio.invoices import (
    invoices_paid_within_date_range,
    invoice_lines_from_invoices,
    invoice_lines_paid_on_fund,
)

logger = logging.getLogger(__name__)
devs_to_email_addr = Variable.get("EMAIL_DEVS")

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email": [devs_to_email_addr],
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
def process_new_funds_group(invoice_lines: list):
    """
    See https://s3.amazonaws.com/foliodocs/api/mod-invoice-storage/p/invoice.html#invoice_storage_invoice_lines_get
    Input: a list of invoice line dictionaries wrapped in a list, e.g. [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]
    """
    paid_bookplate_polines = bookplate_funds_polines(invoice_lines=invoice_lines)
    return instances_from_po_lines(
        po_lines_funds=paid_bookplate_polines
    )  # -> launch_add_979_fields_task


@task.branch()
def date_range_or_funds_path(**kwargs):
    # params["funds"]:
    """
    [
        {
            'druid': 'ef919yq2614',
            'failure': None,
            'fund_name': 'KELP',
            'title': 'The Kelp Foundation Fund',
            'image_filename': 'ef919yq2614_00_0001.jp2',
            'db_id': 4,
            'fund_uuid': 'f916c6e4-1bc7-4892-a5a8-73b8ede6e3a4'
        },
    ]
    """
    params = kwargs.get("params", {})
    funds = params.get("funds", [])
    if len(funds) > 0:
        logger.info(f"New fund {funds}")
        return "invoice_lines_paid_on_fund"
    else:
        return "invoices_paid_within_date_range"


@dag(
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        cron="0 2 * * WED", timezone="America/Los_Angeles"
    ),
    start_date=datetime(2023, 8, 28),
    catchup=False,
    max_active_runs=5,
    tags=["digital bookplates"],
)
def digital_bookplate_instances():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    choose_branch = date_range_or_funds_path()

    # Date range branch
    retrieve_paid_invoices = invoices_paid_within_date_range()
    retrieve_instances = process_date_range_group.expand(
        invoice_id=retrieve_paid_invoices
    )
    launch_add_tag = trigger_digital_bookplate_979_task(instances=retrieve_instances)
    launch_poll_979s = trigger_poll_for_979s_task(dag_runs=launch_add_tag)

    # New funds branch
    paid_invoice_lines_new_fund = invoice_lines_paid_on_fund()
    retrieve_instances_new_fund = process_new_funds_group.expand(
        invoice_lines=paid_invoice_lines_new_fund
    )

    launch_add_tag_new_fund = trigger_digital_bookplate_979_task(
        instances=retrieve_instances_new_fund
    )
    launch_poll_979s_new_fund = trigger_poll_for_979s_task(
        dag_runs=launch_add_tag_new_fund
    )

    start >> choose_branch >> [retrieve_paid_invoices, paid_invoice_lines_new_fund]
    retrieve_paid_invoices >> launch_poll_979s >> end
    paid_invoice_lines_new_fund >> launch_poll_979s_new_fund >> end


digital_bookplate_instances()
