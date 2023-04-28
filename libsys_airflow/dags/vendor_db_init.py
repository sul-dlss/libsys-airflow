import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from plugins.vendor.models import dbinit

logger = logging.getLogger(__name__)


default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "vendor_load_db_init",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 2, 22),
    catchup=False,
    tags=["vendor load", "database"],
    max_active_runs=1,
) as dag:

    initialize_vendor_load_db = PythonOperator(
        task_id="initialize-vendor-load-db",
        python_callable=dbinit,
        op_kwargs={},
    )

    initialize_vendor_load_db
