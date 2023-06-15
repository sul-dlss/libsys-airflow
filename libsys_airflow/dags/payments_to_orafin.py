import logging
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.timetables.interval import CronDataIntervalTimetable

from folioclient import FolioClient

from libsys_airflow.plugins.folio.invoices import (
    invoices_awaiting_payment,
    invoices_pending_payment,
)

from libsys_airflow.plugins.folio.orafin_payments import (
    transform_folio_data,
    feeder_file,
    sftp_file,
)

logger = logging.getLogger(__name__)


def folio_client():
    try:
        return FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )
    except ValueError as error:
        logger.error(error)
        raise


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
    folio_invoices_awaiting_payment = PythonOperator(
        task_id="get_invoices_awaiting_payment",
        python_callable=invoices_awaiting_payment,
    )

    orafin_data = PythonOperator(
        task_id="transform_folio_data_to_orafin_data",
        python_callable=transform_folio_data,
    )

    feeder_file = PythonOperator(
        task_id="write_feeder_file", python_callable=feeder_file
    )

    send_feeder_file = PythonOperator(
        task_id="sftp_feeder_file_to_ap", python_callable=sftp_file
    )

    folio_invoices_pending_payment = PythonOperator(
        # pass voucher UUIDs for invoices sent to AP thru xcom to update voucher disbursementNumber to "Pending"
        task_id="update_invoices_to_pending_payment",
        python_callable=invoices_pending_payment,
    )


(
    folio_invoices_awaiting_payment
    >> orafin_data
    >> feeder_file
    >> send_feeder_file
    >> folio_invoices_pending_payment
)
