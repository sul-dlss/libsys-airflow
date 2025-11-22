from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from libsys_airflow.plugins.airflow.connections import create_connection_task
from libsys_airflow.plugins.vendor.download import (
    filter_by_strategy,
    filter_already_downloaded,
    filter_by_mod_date,
    download_task,
    update_vendor_files_table,
)
from libsys_airflow.plugins.vendor.archive import archive_task
from libsys_airflow.plugins.vendor.paths import download_path
from libsys_airflow.plugins.vendor.emails import files_fetched_email_task

logger = logging.getLogger(__name__)

# mypy: disable-error-code = "index, arg-type"

# Run with:
# docker exec -it libsys-airflow-airflow-worker-1 airflow dags trigger data_fetcher -c '{"vendor_interface_name": "Gobi - Full bibs", "vendor_code": "YANKEE-SUL", "vendor_uuid": "9cce436e-1858-4c37-9c7f-9374a36576ff", "vendor_interface_uuid": "65d30c15-a560-4064-be92-f90e38eeb351", "dataload_profile_uuid": "f4144dbd-def7-4b77-842a-954c62faf319", "remote_path": "oclc", "filename_regex": "^\\d+\\.mrc$"}'
# With processing delay:
# docker exec -it libsys-airflow-airflow-worker-1 airflow dags trigger data_fetcher -c '{"vendor_interface_name": "Gobi - Full bibs", "vendor_code": "YANKEE-SUL", "vendor_uuid": "9cce436e-1858-4c37-9c7f-9374a36576ff", "vendor_interface_uuid": "65d30c15-a560-4064-be92-f90e38eeb351", "dataload_profile_uuid": "f4144dbd-def7-4b77-842a-954c62faf319", "remote_path": "oclc", "filename_regex": "^\\d+\\.mrc$", "processing_delay": 5}'

default_args = dict(
    {
        "owner": "folio",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
)

with DAG(
    dag_id="data_fetcher",
    default_args=default_args,
    schedule=None,
    catchup=False,
    start_date=datetime(2023, 1, 1),
    tags=["vma"],
    params={
        "vendor_interface_name": Param("", type="string"),  # 'Gobi - Full bibs'
        "vendor_code": Param("", type="string"),  # 'YANKEE-SUL'
        "vendor_uuid": Param(
            "", type="string"
        ),  # '9cce436e-1858-4c37-9c7f-9374a36576ff',
        "vendor_interface_uuid": Param(
            "", type="string"
        ),  # '65d30c15-a560-4064-be92-f90e38eeb351',
        "dataload_profile_uuid": Param(
            None, type=["null", "string"]
        ),  # f4144dbd-def7-4b77-842a-954c62faf319
        "remote_path": Param(None, type=["null", "string"]),  # 'oclc'
        "filename_regex": Param(
            None, type=["null", "string"]
        ),  # '^\d+\.mrc$' or CNT-ORD for special Gobi file filtering.
    },
) as dag:

    @task(multiple_outputs=True)
    def setup():
        context = get_current_context()
        params = context["params"]
        dag_run_download_path = download_path(
            params["vendor_uuid"], params["vendor_interface_uuid"]
        )
        params["environment"] = os.getenv('HONEYBADGER_ENVIRONMENT', 'development')

        dag_run_download_path.mkdir(exist_ok=True, parents=True)

        # XCOM cannot serialize pathlib Path object
        params["download_path"] = str(dag_run_download_path)

        logger.info(f"Params are {params}")

        return params

    @task
    def downloaded_files(file_statuses: dict) -> list:
        return [f[0] for f in file_statuses["fetched"]]

    params = setup()
    conn_id = create_connection_task(params["vendor_interface_uuid"])

    file_list_by_strategy = filter_by_strategy(
        conn_id,
        params["remote_path"],
        params["filename_regex"],
    )

    files_not_yet_downloaded = filter_already_downloaded(
        params["remote_path"],
        params["vendor_uuid"],
        params["vendor_interface_uuid"],
        file_list_by_strategy,
    )

    files_to_download = filter_by_mod_date(
        conn_id,
        params["remote_path"],
        params["vendor_uuid"],
        params["vendor_interface_uuid"],
        files_not_yet_downloaded,
    )

    file_statuses = download_task(
        conn_id,
        params["remote_path"],
        params["download_path"],
        params["vendor_interface_name"],
        files_to_download,
    )

    update_vendor_files_table(
        file_statuses, params["vendor_uuid"], params["vendor_interface_uuid"]
    )

    files_to_archive = downloaded_files(file_statuses)

    archive_task(
        files_to_archive,
        params["download_path"],
        params["vendor_uuid"],
        params["vendor_interface_uuid"],
    )

    files_fetched_email_task(files_to_archive, params)
