from datetime import datetime
import logging
import os
import pathlib
from typing import Optional

from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.folio.folio_client import FolioClient
from libsys_airflow.plugins.vendor.models import FileStatus
from libsys_airflow.plugins.vendor.marc import is_marc
from libsys_airflow.plugins.vendor.models import VendorFile

from sqlalchemy.orm import Session


logger = logging.getLogger(__name__)


def record_loading(context):
    _record_status(context, FileStatus.loading)


def record_loaded(context):
    _record_status(context, FileStatus.loaded)


def record_loading_error(context):
    _record_status(context, FileStatus.loading_error)


@task.branch()
def data_import_branch_task(dataload_profile_uuid: Optional[str]):
    if dataload_profile_uuid:
        return "data_import_task"
    else:
        return None


@task(
    on_execute_callback=record_loading,
    on_success_callback=record_loaded,
    on_failure_callback=record_loading_error,
    max_active_tis_per_dag=Variable.get("max_active_data_import_tis", default_var=1),
)
def data_import_task(
    download_path: str,
    batch_filenames: list[str],
    dataload_profile_uuid: str,
    vendor_interface_uuid: str,
    filename: str,
):
    """
    Imports a file into Folio using Data Import API
    """
    data_import(
        download_path,
        batch_filenames,
        dataload_profile_uuid,
        vendor_interface_uuid,
        filename,
    )


def data_import(
    download_path,
    batch_filenames,
    dataload_profile_uuid,
    vendor_interface_uuid,
    filename,
    folio_client=None,
):
    client = folio_client or _folio_client()
    upload_definition_id, job_execution_id, file_definition_dict = _upload_definition(
        client, batch_filenames
    )

    upload_definition = None
    for chunked_filename, file_definition_id in file_definition_dict.items():
        logger.info(f"Uploading {chunked_filename}")
        upload_definition = _upload_file(
            client,
            os.path.join(download_path, chunked_filename),
            upload_definition_id,
            file_definition_id,
        )
    data_type = _data_type(download_path, batch_filenames[0])
    logger.info(f"Data type: {data_type}")
    _process_files(
        client,
        upload_definition_id,
        dataload_profile_uuid,
        upload_definition,
        data_type,
    )
    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        vendor_file = VendorFile.load(vendor_interface_uuid, filename, session)
        vendor_file.folio_job_execution_uuid = job_execution_id
        session.commit()
    logger.info(
        f"Data import job started for {batch_filenames} with job_execution_id {job_execution_id}"
    )


def _folio_client():
    return FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )


def _upload_definition(folio_client, filenames):
    payload = {"fileDefinitions": list({"name": filename} for filename in filenames)}
    resp_json = folio_client.post("/data-import/uploadDefinitions", payload)
    upload_definition_id = resp_json["fileDefinitions"][0]["uploadDefinitionId"]
    logger.info(f"upload_definition_id: {upload_definition_id}")
    job_execution_id = resp_json["fileDefinitions"][0]["jobExecutionId"]
    logger.info(f"job_execution_id: {job_execution_id}")
    file_definition_dict = dict()
    for file_definition in resp_json["fileDefinitions"]:
        file_definition_dict[file_definition["name"]] = file_definition["id"]
    logger.info(f"file_definition_dict: {file_definition_dict}")

    return upload_definition_id, job_execution_id, file_definition_dict


def _upload_file(folio_client, filepath, upload_definition_id, file_definition_id):
    return folio_client.post_file(
        f"/data-import/uploadDefinitions/{upload_definition_id}/files/{file_definition_id}",
        filepath,
    )


def _process_files(
    folio_client,
    upload_definition_id,
    job_profile_uuid,
    upload_definition,
    data_type,
):
    payload = {
        "uploadDefinition": upload_definition,
        "jobProfileInfo": {"id": job_profile_uuid, "dataType": data_type},
    }
    folio_client.post(
        f"/data-import/uploadDefinitions/{upload_definition_id}/processFiles", payload
    )


def _record_status(context, status: FileStatus):
    vendor_interface_uuid = context["params"]["vendor_interface_uuid"]
    filename = context["params"]["filename"]

    logger.info(f"Recording {status} for {filename}")

    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        vendor_file = VendorFile.load(vendor_interface_uuid, filename, session)
        vendor_file.status = status
        if status is FileStatus.loaded:
            vendor_file.loaded_timestamp = datetime.utcnow()
        session.commit()


def _data_type(download_path, filename):
    if is_marc(pathlib.Path(download_path) / filename):
        return "MARC"
    else:
        return "EDIFACT"
