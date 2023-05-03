import pytest  # noqa
from unittest.mock import MagicMock

import os
import shutil

from libsys_airflow.plugins.folio.data_import import data_import


@pytest.fixture
def folio_client():
    upload_definition_resp = {
        "id": "38f47152-c3c2-471c-b7e0-c9d024e47357",
        "metaJobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
        "status": "NEW",
        "createDate": "2023-05-04T18:14:20.541+00:00",
        "fileDefinitions": [
            {
                "id": "8b5cc830-0c3d-496a-8472-3b98aa40d109",
                "name": "0720230118.mrc",
                "status": "NEW",
                "jobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
                "uploadDefinitionId": "38f47152-c3c2-471c-b7e0-c9d024e47357",
                "createDate": "2023-05-04T18:14:20.541+00:00",
            }
        ],
        "metadata": {
            "createdDate": "2023-05-04T18:14:20.187+00:00",
            "createdByUserId": "3e2ed889-52f2-45ce-8a30-8767266f07d2",
            "updatedDate": "2023-05-04T18:14:20.187+00:00",
            "updatedByUserId": "3e2ed889-52f2-45ce-8a30-8767266f07d2",
        },
    }
    upload_file_resp = {
        "id": "38f47152-c3c2-471c-b7e0-c9d024e47357",
        "metaJobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
        "status": "LOADED",
        "createDate": "2023-05-04T18:14:20.541+00:00",
        "fileDefinitions": [
            {
                "id": "8b5cc830-0c3d-496a-8472-3b98aa40d109",
                "sourcePath": "./storage/upload/38f47152-c3c2-471c-b7e0-c9d024e47357/8b5cc830-0c3d-496a-8472-3b98aa40d109/0720230118.mrc",
                "name": "0720230118.mrc",
                "status": "UPLOADED",
                "jobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
                "uploadDefinitionId": "38f47152-c3c2-471c-b7e0-c9d024e47357",
                "createDate": "2023-05-04T18:14:20.541+00:00",
                "uploadedDate": "2023-05-04T18:14:21.736+00:00",
            }
        ],
        "metadata": {
            "createdDate": "2023-05-04T18:14:20.187+00:00",
            "createdByUserId": "3e2ed889-52f2-45ce-8a30-8767266f07d2",
            "updatedDate": "2023-05-04T18:14:20.187+00:00",
            "updatedByUserId": "3e2ed889-52f2-45ce-8a30-8767266f07d2",
        },
    }

    mock_client = MagicMock()
    mock_client.post.side_effect = [upload_definition_resp, None]
    mock_client.post_file.return_value = upload_file_resp
    return mock_client


@pytest.fixture
def filename():
    return "0720230118.mrc"


@pytest.fixture
def download_path(tmp_path):
    dest_filepath = os.path.join(tmp_path, "0720230118.mrc")
    shutil.copyfile("tests/vendor/0720230118.mrc", dest_filepath)
    return str(tmp_path)


def test_data_import(download_path, folio_client):
    data_import(
        download_path,
        "0720230118.mrc",
        "F4144dbd-def7-4b77-842a-954c62faf319",
        folio_client=folio_client,
    )

    folio_client.post.assert_any_call(
        "/data-import/uploadDefinitions",
        {"fileDefinitions": [{"name": "0720230118.mrc"}]},
    )
    folio_client.post_file.assert_called_once_with(
        "/data-import/uploadDefinitions/38f47152-c3c2-471c-b7e0-c9d024e47357/files/8b5cc830-0c3d-496a-8472-3b98aa40d109",
        os.path.join(download_path, "0720230118.mrc"),
    )

    process_files_payload = {
        "uploadDefinition": {
            "id": "38f47152-c3c2-471c-b7e0-c9d024e47357",
            "metaJobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
            "status": "LOADED",
            "createDate": "2023-05-04T18:14:20.541+00:00",
            "fileDefinitions": [
                {
                    "id": "8b5cc830-0c3d-496a-8472-3b98aa40d109",
                    "sourcePath": "./storage/upload/38f47152-c3c2-471c-b7e0-c9d024e47357/8b5cc830-0c3d-496a-8472-3b98aa40d109/0720230118.mrc",
                    "name": "0720230118.mrc",
                    "status": "UPLOADED",
                    "jobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
                    "uploadDefinitionId": "38f47152-c3c2-471c-b7e0-c9d024e47357",
                    "createDate": "2023-05-04T18:14:20.541+00:00",
                    "uploadedDate": "2023-05-04T18:14:21.736+00:00",
                }
            ],
            "metadata": {
                "createdDate": "2023-05-04T18:14:20.187+00:00",
                "createdByUserId": "3e2ed889-52f2-45ce-8a30-8767266f07d2",
                "updatedDate": "2023-05-04T18:14:20.187+00:00",
                "updatedByUserId": "3e2ed889-52f2-45ce-8a30-8767266f07d2",
            },
        },
        "jobProfileInfo": {
            "id": "F4144dbd-def7-4b77-842a-954c62faf319",
            "dataType": "MARC",
        },
    }

    folio_client.post.assert_called_with(
        "/data-import/uploadDefinitions/38f47152-c3c2-471c-b7e0-c9d024e47357/processFiles",
        process_files_payload,
    )
