from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.folio.data_import import (
    data_import_task,
    data_import_branch_task,
)
from libsys_airflow.plugins.vendor.emails import (
    file_loaded_email_task,
    file_not_loaded_email_task,
)
from libsys_airflow.plugins.vendor.extract import extract_task
from libsys_airflow.plugins.vendor.file_loaded_sensor import file_loaded_sensor_task
from libsys_airflow.plugins.vendor.job_summary import job_summary_task
from libsys_airflow.plugins.vendor.marc import process_marc_task, batch_task
from libsys_airflow.plugins.vendor.models import VendorInterface, FileStatus
from libsys_airflow.plugins.vendor.paths import download_path
from libsys_airflow.plugins.vendor.file_status import record_status_from_context

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# mypy: disable-error-code = "index, arg-type"

# Run with:
# docker exec -it libsys-airflow-airflow-worker-1 airflow dags trigger default_data_processor -c '{"vendor_uuid": "9cce436e-1858-4c37-9c7f-9374a36576ff", "vendor_interface_uuid": "65d30c15-a560-4064-be92-f90e38eeb351", "dataload_profile_uuid": "f4144dbd-def7-4b77-842a-954c62faf319", "filename": "0720230403.mrc"}'

default_args = dict(
    {
        "owner": "folio",
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
)

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

    def record_processing(context):
        record_status_from_context(context, FileStatus.processing)

    @task(
        multiple_outputs=True,
        on_execute_callback=record_processing,
    )
    def setup():
        context = get_current_context()
        params = context["params"]
        params["start_time"] = context["dag_run"].start_date
        params["download_path"] = download_path(
            params["vendor_uuid"], params["vendor_interface_uuid"]
        )
        params["environment"] = os.getenv('HONEYBADGER_ENVIRONMENT', 'development')

        pg_hook = PostgresHook("vendor_loads")
        with Session(pg_hook.get_sqlalchemy_engine()) as session:
            vendor_interface = VendorInterface.load_with_vendor(
                params["vendor_uuid"], params["vendor_interface_uuid"], session
            )
            params["vendor_code"] = vendor_interface.vendor.vendor_code_from_folio
            params["vendor_interface_name"] = vendor_interface.display_name
            # Map from processing options to params.
            processing_options = vendor_interface.processing_options or {}
            # Processing options might look like this:
            # {"package_name": "", "change_marc": [], "delete_marc": []}
            # {"package_name": "coutts", "change_marc": [{"from": "520", "to": "920"}], "delete_marc": ["123"]}
            params["change_fields"] = processing_options.get("change_marc", [])
            params["remove_fields"] = processing_options.get("delete_marc") or [
                "905",
                "920",
                "986",
            ]  # Casts [] to defaults.
            add_fields = []
            package_name = processing_options.get("package_name")
            if package_name:
                add_fields.append(
                    {
                        "tag": "910",
                        "subfields": [{"code": "a", "value": package_name}],
                    }
                )
            if vendor_interface.vendor.vendor_code_from_folio == "sfx":
                add_fields.append(
                    {
                        "tag": "590",
                        "subfields": [{"code": "a", "value": "MARCit brief record."}],
                        "unless": {
                            "tag": "035",
                            "subfields": [{"code": "a", "value": "OCoLC"}],
                        },
                    }
                )

            params["add_fields"] = add_fields or None

            # Not yet supported in UI.
            params["archive_regex"] = processing_options.get("archive_regex")

        logger.info(f"Params are {params}")
        assert (params["download_path"] / params["filename"]).exists()
        # XCOM cannot serialize pathlib Paths
        params["download_path"] = str(params["download_path"])

        return params

    params = setup()
    filename = extract_task(
        params["download_path"], params["filename"], params["archive_regex"]
    )
    processed_params = process_marc_task(
        params["download_path"],
        filename,
        params["remove_fields"],
        params["change_fields"],
        params["add_fields"],
    )

    batch_filenames = batch_task(params["download_path"], processed_params["filename"])
    data_import_branch = data_import_branch_task(params["dataload_profile_uuid"])
    data_import = data_import_task(
        params["download_path"],
        batch_filenames,
        params["dataload_profile_uuid"],
        params["vendor_uuid"],
        params["vendor_interface_uuid"],
        params["filename"],
    )

    file_loaded_sensor = file_loaded_sensor_task(
        params["vendor_interface_uuid"],
        params["filename"],
        data_import["job_execution_id"],
    )

    job_summary = job_summary_task(data_import["job_execution_id"])

    file_loaded_email_task(processed_params=processed_params)

    file_not_loaded_email = file_not_loaded_email_task(
        processed_params=processed_params
    )

    data_import_branch >> data_import >> file_loaded_sensor >> job_summary
    data_import_branch >> file_not_loaded_email
