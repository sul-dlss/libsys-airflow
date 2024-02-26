from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable

import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    "select_oclc_records",
    default_args=default_args,
    schedule=timedelta(days=(Variable.get("schedule_oclc_days", 7))),
    start_date=datetime(2024, 2, 25),
    catchup=False,
    tags=["data_exports"],
) as dag:

    # Sample methods to be removed and replaced by real methods, along with imports when they are coded.
    def fetch_marc_record_ids():
        logger.info("Replace this with method from record selection module")

    def folio_marc_records_for_id():
        logger.info("Replace this with method from marc module")

    def sample_marc_transform_1():
        logger.info("Replace this with method from marc processing module")

    def save_transformed_marc():
        logger.info("Replace this with method from marc writing module")

    fetch_record_ids = PythonOperator(
        task_id="fetch_record_ids_from_folio",
        python_callable=fetch_marc_record_ids,
        op_kwargs={},
    )

    fetch_marc_records = PythonOperator(
        task_id="fetch_marc_records_from_folio",
        python_callable=folio_marc_records_for_id,
        op_kwargs={},
    )

    transform_marc_record = PythonOperator(
        task_id="transform_folio_marc_record",
        python_callable=sample_marc_transform_1,
        op_kwargs={},
    )

    write_marc_to_fs = PythonOperator(
        task_id="write_marc_record_to_file",
        python_callable=save_transformed_marc,
        op_kwargs={},
    )


fetch_record_ids >> fetch_marc_records >> transform_marc_record >> write_marc_to_fs
