import logging
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    gather_files_task,
    transmit_data_http_task,
)

logger = logging.getLogger(__name__)

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
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data export"],
    params={
        "vendor": Param(
            "pod",
            type="string",
            description="Send all records to this vendor.",
            enum=["pod", "sharevde"],
        ),
        "bucket": Param(
            Variable.get("FOLIO_AWS_BUCKET", "folio-data-export-prod"), type="string"
        ),
    },
)
def send_all_records():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    gather_files = gather_files_task(vendor="full-dump")

    transmit_data = transmit_data_http_task(
        gather_files,
        files_params="upload[files][]",
    )

    start >> gather_files >> transmit_data >> end


send_all_records()
