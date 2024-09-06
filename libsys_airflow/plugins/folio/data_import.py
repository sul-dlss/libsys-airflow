import copy
import logging
import os
import pathlib
from typing import Optional

from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from folioclient import FolioClient

from libsys_airflow.plugins.vendor.models import FileStatus
from libsys_airflow.plugins.vendor.marc import is_marc
from libsys_airflow.plugins.vendor.models import VendorFile, VendorInterface
from libsys_airflow.plugins.vendor.file_status import record_status_from_context

from sqlalchemy.orm import Session


logger = logging.getLogger(__name__)


def record_loading(context):
    record_status_from_context(context, FileStatus.loading)


def record_loaded(context):
    record_status_from_context(context, FileStatus.loaded)


def record_loading_error(context):
    record_status_from_context(context, FileStatus.loading_error)


@task.branch()
def data_import_branch_task(dataload_profile_uuid: Optional[str]):
    if dataload_profile_uuid:
        return "data_import_task"
    else:
        return "file_not_loaded_email_task"


@task(
    on_execute_callback=record_loading,
    on_success_callback=record_loaded,
    on_failure_callback=record_loading_error,
    max_active_tis_per_dag=Variable.get("max_active_data_import_tis", default_var=1),
    multiple_outputs=True,
)
def data_import_task(
    download_path: str,
    batch_filenames: list[str],
    dataload_profile_uuid: str,
    vendor_uuid: str,
    vendor_interface_uuid: str,
    filename: str,
) -> dict:
    """
    Imports a file into Folio using Data Import API
    """
    return data_import(
        download_path,
        batch_filenames,
        dataload_profile_uuid,
        vendor_uuid,
        vendor_interface_uuid,
        filename,
    )


def data_import(
    download_path,
    batch_filenames,
    dataload_profile_uuid,
    vendor_uuid,
    vendor_interface_uuid,
    filename,
    folio_client=None,
) -> dict:
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
        vendor_interface = VendorInterface.load_with_vendor(
            vendor_uuid, vendor_interface_uuid, session
        )
        vendor_file = VendorFile.load_with_vendor_interface(
            vendor_interface, filename, session
        )
        vendor_file.folio_job_execution_uuid = job_execution_id
        session.commit()
    logger.info(
        f"Data import job started for {batch_filenames} with job_execution_id {job_execution_id}"
    )
    return {
        # NOTE: We think we don't need this anymore, but we don't have absolute
        #       confidence in this, so we're leaving this here for now
        #
        # "upload_definition_id": upload_definition_id,
        "job_execution_id": job_execution_id,
    }


def _folio_client():
    return FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )


def _upload_definition(folio_client, filenames):
    payload = {"fileDefinitions": list({"name": filename} for filename in filenames)}
    resp_json = folio_client.folio_post("/data-import/uploadDefinitions", payload)
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
    url = f"{folio_client.okapi_url}/data-import/uploadDefinitions/{upload_definition_id}/files/{file_definition_id}"
    headers = copy.deepcopy(folio_client.okapi_headers)
    headers["content-type"] = "application/octet-stream"

    with open(filepath, 'rb') as fo:
        payload = fo.read()

    with folio_client.get_folio_http_client() as httpx_client:
        result = httpx_client.post(url, headers=headers, data=payload)

    result.raise_for_status()
    return result.json()


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
    folio_client.folio_post(
        f"/data-import/uploadDefinitions/{upload_definition_id}/processFiles", payload
    )


def _data_type(download_path, filename):
    if is_marc(pathlib.Path(download_path) / filename):
        return "MARC"
    else:
        return "EDIFACT"
