import logging
from datetime import datetime, timedelta

from airflow.models.connection import Connection
from airflow.sdk import (
    dag,
    task,
    Param,
    Variable,
)
from airflow.providers.standard.operators.empty import EmptyOperator

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    gather_files_task,
    retry_failed_files_task,
    transmit_data_http_task,
    transmit_data_ftp_task,
)

from libsys_airflow.plugins.data_exports.email import (
    failed_transmission_email,
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


@task(multiple_outputs=True)
def retrieve_params(**kwargs):
    """
    Determine connection type based on "vendor" Param
    """
    params = kwargs.get("params", {})
    conn_id = params["vendor"]
    return {"conn_id": conn_id}


@task.branch()
def http_or_ftp_path(**kwargs):
    """
    Determine transmission type based on conn_type from connection
    """
    conn_id = kwargs.get("connection")
    logger.info(f"Send all records to vendor {conn_id}")
    connection = Connection.get_connection_from_secrets(conn_id)
    conn_type = connection.conn_type
    logger.info(f"Transmit data via {conn_type}")
    if conn_type == "http":
        return "transmit_data_http_task"
    else:
        return "transmit_data_ftp_task"


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
            enum=["pod", "sharevde", "backstage"],
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

    vars = retrieve_params()

    choose_branch = http_or_ftp_path(connection=vars["conn_id"])

    # http branch
    transmit_data = transmit_data_http_task(
        gather_files,
        files_params="upload[files][]",
    )

    retry_files = retry_failed_files_task(
        vendor="full-dump", files=transmit_data["failures"]
    )

    retry_transmission = transmit_data_http_task(
        retry_files,
        files_params="upload[files][]",
    )

    email_failures = failed_transmission_email(retry_transmission["failures"])

    # ftp branch
    transmit_data_ftp = transmit_data_ftp_task(vars["conn_id"], gather_files)
    retry_files_ftp = retry_failed_files_task(
        vendor="full-dump", files=transmit_data_ftp["failures"]
    )
    retry_transmit_data_ftp = transmit_data_ftp_task(vars["conn_id"], retry_files_ftp)
    email_failures_ftp = failed_transmission_email(retry_transmit_data_ftp["failures"])

    start >> gather_files >> vars >> choose_branch >> [transmit_data, transmit_data_ftp]
    transmit_data >> retry_files >> retry_transmission >> email_failures >> end
    (
        transmit_data_ftp
        >> retry_files_ftp
        >> retry_transmit_data_ftp
        >> email_failures_ftp
        >> end
    )


send_all_records()
