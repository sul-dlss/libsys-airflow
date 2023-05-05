from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from plugins.airflow.connections import create_connection_task
from plugins.vendor.download import ftp_download_task
from plugins.vendor.archive import archive_task
from plugins.vendor.paths import download_path, archive_path

logger = logging.getLogger(__name__)

# Run with:
# docker exec -it libsys-airflow-airflow-worker-1 airflow dags trigger data_fetcher -c '{"vendor_name": "GOBI/YBP", "vendor_code": "YANKEE-SUL", "vendor_uuid": "9cce436e-1858-4c37-9c7f-9374a36576ff", "vendor_interface_uuid": "65d30c15-a560-4064-be92-f90e38eeb351", "dataload_profile_uuid": "f4144dbd-def7-4b77-842a-954c62faf319", "remote_path": "oclc", "filename_regex": "^\\d+\\.mrc$"}'

default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_fetcher",
    default_args=default_args,
    schedule=None,
    catchup=False,
    start_date=datetime(2023, 1, 1),
    params={
        "vendor_name": Param("", type="string"),  # 'GOBI/YBP'
        "vendor_code": Param("", type="string"),  # 'YANKEE-SUL'
        "vendor_uuid": Param(
            "", type="string"
        ),  # '9cce436e-1858-4c37-9c7f-9374a36576ff',
        "vendor_interface_uuid": Param(
            "", type="string"
        ),  # '65d30c15-a560-4064-be92-f90e38eeb351',
        "dataload_profile_uuid": Param(
            "", type="string"
        ),  # f4144dbd-def7-4b77-842a-954c62faf319
        "remote_path": Param("", type="string"),  # 'oclc'
        "filename_regex": Param(None, type=["null", "string"]),  # '^\d+\.mrc$'
    },
) as dag:

    @task(multiple_outputs=True)
    def setup():
        context = get_current_context()
        params = context["params"]
        params["download_path"] = download_path(
            params["vendor_uuid"], params["vendor_interface_uuid"]
        )
        params["archive_path"] = archive_path(
            params["vendor_uuid"],
            params["vendor_interface_uuid"],
            context["execution_date"],
        )

        logger.info(f"Params are {params}")

        os.makedirs(params["download_path"], exist_ok=True)
        os.makedirs(params["archive_path"], exist_ok=True)

        return params

    @task
    def generate_dag_run_conf(
        downloaded_files: list,
        vendor_uuid: str,
        vendor_interface_uuid: str,
        dataload_profile_uuid: str,
    ) -> list[dict]:
        """
        Generate a DAG conf for each downloaded file.
        """
        confs = []
        for filename in downloaded_files:
            confs.append(
                {
                    "vendor_uuid": vendor_uuid,
                    "vendor_interface_uuid": vendor_interface_uuid,
                    "dataload_profile_uuid": dataload_profile_uuid,
                    "filename": filename,
                }
            )
        return confs

    params = setup()
    conn_id = create_connection_task(params["vendor_interface_uuid"])
    downloaded_files = ftp_download_task(
        conn_id,
        params["remote_path"],
        params["download_path"],
        params["filename_regex"],
    )
    archive_task(downloaded_files, params["download_path"], params["archive_path"])

    dag_run_confs = generate_dag_run_conf(
        downloaded_files,
        params["vendor_uuid"],
        params["vendor_interface_uuid"],
        params["dataload_profile_uuid"],
    )
    TriggerDagRunOperator.partial(
        task_id="process_file", trigger_dag_id="default_data_processor"
    ).expand(conf=dag_run_confs)
