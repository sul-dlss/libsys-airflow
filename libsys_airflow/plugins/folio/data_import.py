import logging
import os

from airflow.models import Variable
from airflow.decorators import task

from libsys_airflow.plugins.folio.folio_client import FolioClient

logger = logging.getLogger(__name__)


@task
def data_import_task(download_path: str, filename: str, dataload_profile_uuid: str):
    """
    Imports a file into Folio using Data Import API
    """
    data_import(download_path, filename, dataload_profile_uuid)


def data_import(download_path, filename, dataload_profile_uuid, folio_client=None):
    client = folio_client or _folio_client()
    upload_definition_id, job_execution_id, file_definition_id = _upload_definition(
        client, filename
    )

    upload_definition = _upload_file(
        client,
        os.path.join(download_path, filename),
        upload_definition_id,
        file_definition_id,
    )

    _process_files(
        client, upload_definition, upload_definition_id, dataload_profile_uuid
    )
    logger.info(
        f"Data import job started for {filename} with job_execution_id {job_execution_id}"
    )


def _folio_client():
    return FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )


def _upload_definition(folio_client, filename):
    payload = {"fileDefinitions": [{"name": filename}]}
    resp_json = folio_client.post("/data-import/uploadDefinitions", payload)
    upload_definition_id = resp_json["fileDefinitions"][0]["uploadDefinitionId"]
    logger.info(f"upload_definition_id: {upload_definition_id}")
    job_execution_id = resp_json["fileDefinitions"][0]["jobExecutionId"]
    logger.info(f"job_execution_id: {job_execution_id}")
    file_definition_id = resp_json["fileDefinitions"][0]["id"]
    logger.info(f"file_definition_id: {file_definition_id}")

    return upload_definition_id, job_execution_id, file_definition_id


def _upload_file(folio_client, filepath, upload_definition_id, file_definition_id):
    return folio_client.post_file(
        f"/data-import/uploadDefinitions/{upload_definition_id}/files/{file_definition_id}",
        filepath,
    )


def _process_files(
    folio_client,
    upload_definition,
    upload_definition_id,
    job_profile_uuid,
    data_type="MARC",
):
    payload = {
        "uploadDefinition": upload_definition,
        "jobProfileInfo": {"id": job_profile_uuid, "dataType": data_type},
    }
    folio_client.post(
        f"/data-import/uploadDefinitions/{upload_definition_id}/processFiles", payload
    )
