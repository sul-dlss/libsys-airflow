from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.vendor.marc import process_marc_task, batch_task
from libsys_airflow.plugins.vendor.paths import download_path
from libsys_airflow.plugins.folio.data_import import (
    data_import_task,
    data_import_branch_task,
)
from libsys_airflow.plugins.vendor.extract import extract_task
from libsys_airflow.plugins.vendor.models import VendorFile
from libsys_airflow.plugins.vendor.vendor_interface import load_vendor_interface

from sqlalchemy.orm import Session
from sqlalchemy import select

logger = logging.getLogger(__name__)

# Run with:
# docker exec -it libsys-airflow-airflow-worker-1 airflow dags trigger default_data_processor -c '{"vendor_uuid": "9cce436e-1858-4c37-9c7f-9374a36576ff", "vendor_interface_uuid": "65d30c15-a560-4064-be92-f90e38eeb351", "dataload_profile_uuid": "f4144dbd-def7-4b77-842a-954c62faf319", "filename": "0720230403.mrc"}'

default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="default_data_processor",
    default_args=default_args,
    schedule=None,
    catchup=False,
    start_date=datetime(2023, 1, 1),
    params={
        "vendor_uuid": Param(
            "", type="string"
        ),  # '9cce436e-1858-4c37-9c7f-9374a36576ff',
        "vendor_interface_uuid": Param(
            "", type="string"
        ),  # '65d30c15-a560-4064-be92-f90e38eeb351',
        "dataload_profile_uuid": Param(
            None, type=["null", "string"]
        ),  # f4144dbd-def7-4b77-842a-954c62faf319
        "filename": Param("", type="string"),
    },
) as dag:

    @task(multiple_outputs=True)
    def setup():
        context = get_current_context()
        params = context["params"]
        params["download_path"] = download_path(
            params["vendor_uuid"], params["vendor_interface_uuid"]
        )

        pg_hook = PostgresHook("vendor_loads")
        with Session(pg_hook.get_sqlalchemy_engine()) as session:
            vendor_interface = load_vendor_interface(
                params["vendor_interface_uuid"], session
            )
            processing_options = vendor_interface.processing_options or {}
            params["change_fields"] = processing_options.get("change_fields")
            params["remove_fields"] = processing_options.get(
                "remove_fields", ["905", "920", "986"]
            )
            params["add_fields"] = processing_options.get("add_fields")
            params["archive_regex"] = processing_options.get("archive_regex")

            vendor_file = session.scalars(
                select(VendorFile)
                .where(VendorFile.vendor_filename == params["filename"])
                .where(VendorFile.vendor_interface_id == vendor_interface.id)
            ).first()
            logger.info(f"vendor_file is {vendor_file}")
            vendor_file.dag_run_id = context["run_id"]
            session.commit()

        logger.info(f"Params are {params}")
        assert os.path.exists(os.path.join(params["download_path"], params["filename"]))

        return params

    params = setup()
    filename = extract_task(
        params["download_path"], params["filename"], params["archive_regex"]
    )
    process_marc = process_marc_task(
        params["download_path"],
        filename,
        params["remove_fields"],
        params["change_fields"],
        params["add_fields"],
    )
    batch_filenames = batch_task(params["download_path"], filename)
    data_import_branch = data_import_branch_task(params["dataload_profile_uuid"])
    data_import = data_import_task(
        params["download_path"], batch_filenames, params["dataload_profile_uuid"]
    )

    process_marc >> batch_filenames
    data_import_branch >> data_import
