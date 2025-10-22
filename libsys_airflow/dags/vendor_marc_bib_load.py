from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG

from airflow.providers.standard.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)


default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="vendor_marc_bib_load",
    default_args=default_args,
    schedule="0 0 * * *",
    catchup=False,
    start_date=datetime(2023, 1, 1),
) as dag:
    get_vendor_data = EmptyOperator(task_id="get_vendor_data")

    get_vendor_connection = EmptyOperator(task_id="get_vendor_connection")

    trigger_vendor_dag = EmptyOperator(task_id="trigger_vendor_dag")

    get_vendor_data >> get_vendor_connection >> trigger_vendor_dag
