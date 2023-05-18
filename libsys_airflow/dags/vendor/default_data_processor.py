from datetime import datetime, timedelta
import logging
import os

from sqlalchemy.orm import Session
from sqlalchemy import select

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.vendor.marc import filter_fields_task, batch_task
from libsys_airflow.plugins.vendor.paths import download_path
from libsys_airflow.plugins.vendor.models import VendorFile, VendorInterface, FileDataLoad
from libsys_airflow.plugins.folio.data_import import data_import_task

logger = logging.getLogger(__name__)

# Run with:
# docker exec -it libsys-airflow-airflow-worker-1 airflow dags trigger default_data_processor -c '{"vendor_uuid": "9cce436e-1858-4c37-9c7f-9374a36576ff", "vendor_interface_uuid": "65d30c15-a560-4064-be92-f90e38eeb351", "dataload_profile_uuid": "F4144dbd-def7-4b77-842a-954c62faf319", "filename": "0720230403.mrc"}'

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
            "", type="string"
        ),  # f4144dbd-def7-4b77-842a-954c62faf319
        "filename": Param("", type="string"),
    },
) as dag:

    @task(multiple_outputs=True)
    def setup():
        context = get_current_context()
        logger.info(f"Context has dag run_id {context['run_id']}")
        params = context["params"]
        params["download_path"] = download_path(
            params["vendor_uuid"], params["vendor_interface_uuid"]
        )

        logger.info(f"Params are {params}")

        assert os.path.exists(os.path.join(params["download_path"], params["filename"]))

        # create a FileDataLoad record, before the load occurs
        pg_hook = PostgresHook("vendor_loads")
        with Session(pg_hook.get_sqlalchemy_engine()) as session:
            vendor_interface = session.scalars(
                select(VendorInterface).where(
                    VendorInterface.folio_interface_uuid == params["vendor_interface_uuid"]
                )
            ).first()
            logger.info(f"vendor_interface is {vendor_interface}")
            vendor_file = session.scalars(
                select(VendorFile)
                .where(VendorFile.vendor_filename == params["filename"])
                .where(VendorFile.vendor_interface_id == vendor_interface.id)
            ).first()
            logger.info(f"vendor_file is {vendor_file}")
            new_file_data_load = FileDataLoad(
            created=datetime.now(),
            updated=datetime.now(),
            vendor_file_id=vendor_file.id,
            dag_run_id=context["run_id"], 
            )

            session.add(new_file_data_load)
            session.commit()

        return params

    params = setup()
    filter_fields = filter_fields_task(params["download_path"], params["filename"])
    batch_filenames = batch_task(params["download_path"], params["filename"])
    data_import = data_import_task(
        params["download_path"], batch_filenames, params["dataload_profile_uuid"]
    )

    filter_fields >> batch_filenames
