import logging
from datetime import datetime, timedelta

from airflow import DAG

from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.folio.invoices import (
    invoices_awaiting_payment_task,
    invoices_pending_payment_task,
)

from libsys_airflow.plugins.folio.orafin_payments import (
    transform_folio_data_task,
    email_excluded_task,
    feeder_file_task,
    filter_invoices_task,
    sftp_file_task,
)

logger = logging.getLogger(__name__)


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "payments_to_orafin",
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        cron="00 18 * * 3,5", timezone="America/Los_Angeles"
    ),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["folio"],
) as dag:
    folio_invoice_ids = invoices_awaiting_payment_task()

    orafin_data = transform_folio_data_task.expand(invoice_id=folio_invoice_ids)

    filtered_invoices = filter_invoices_task(orafin_data)

    feeder_file = feeder_file_task(filtered_invoices["feed"])

    upload_status = sftp_file_task(feeder_file)

    email_excluded_invoices = email_excluded_task(filtered_invoices["excluded"])

    invoices_pending_payment_task(folio_invoice_ids, upload_status)
