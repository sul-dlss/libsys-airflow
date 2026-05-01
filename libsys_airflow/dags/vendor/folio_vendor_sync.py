from datetime import datetime, timedelta
import logging

from airflow.sdk import (
    DAG,
    Param,
    get_current_context,
    task,
)
from libsys_airflow.plugins.vendor.sync import sync_data_task


logger = logging.getLogger(__name__)

# mypy: disable-error-code = index

# Run with:
# docker exec -it libsys-airflow-airflow-worker-1 airflow dags trigger folio_vendor_sync -c '{"folio_org_uuid": "57705203-3413-40aa-9bd5-35fafc6d72d7"}'

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
    tags=["vma"],
    params={
        "folio_org_uuid": Param(None, type=["null", "string"])
    },  # "57705203-3413-40aa-9bd5-35fafc6d72d7"
) as dag:

    @task(multiple_outputs=True)
    def setup():
        context = get_current_context()
        params = context["params"]
        logger.info(f"Params are {params}")
        return params

    params = setup()

    sync_data_task(params["folio_org_uuid"])
