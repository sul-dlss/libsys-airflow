from datetime import datetime, timedelta
import logging

from airflow import DAG
from libsys_airflow.plugins.vendor.sync import sync_data_task


logger = logging.getLogger(__name__)

# Run with:
# docker exec -it libsys-airflow-airflow-worker-1 airflow dags trigger folio_vendor_sync

default_args = dict(
    {
        "owner": "folio",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
)

with DAG(
    dag_id="folio_vendor_sync",
    default_args=default_args,
    schedule=None,  # change to daily?
    catchup=False,
    start_date=datetime(2023, 1, 1),
) as dag:
    sync_data = sync_data_task()
