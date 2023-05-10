import pytest  # noqa
from datetime import datetime
from unittest.mock import MagicMock
from pytest_mock_resources import create_sqlite_fixture, Rows

import os
import shutil

from libsys_airflow.plugins.folio.data_import import (
    data_import,
    record_loading,
    record_loaded,
    record_loading_error,
)
from libsys_airflow.plugins.vendor.models import VendorInterface, VendorFile, FileStatus

from sqlalchemy.orm import Session
from sqlalchemy import select

from airflow.providers.postgres.hooks.postgres import PostgresHook

rows = Rows(
    VendorInterface(
        id=1,
        display_name="Gobi - Full bibs",
        folio_interface_uuid="65d30c15-a560-4064-be92-f90e38eeb351",
        folio_data_import_profile_uuid="f4144dbd-def7-4b77-842a-954c62faf319",
        file_pattern=r"^\d+\.mrc$",
        remote_path="oclc",
        active=True,
    ),
    VendorFile(
        created=datetime.now(),
        updated=datetime.now(),
        vendor_interface_id=1,
        vendor_filename="3820230411.mrc",
        filesize=234,
        status=FileStatus.not_fetched,
        vendor_timestamp=datetime.fromisoformat("2022-01-01T00:05:23"),
    ),
)

engine = create_sqlite_fixture(rows)


@pytest.fixture
def pg_hook(mocker, engine) -> PostgresHook:
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    return mock_hook


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
                "name": "0720230118_1.mrc",
                "status": "NEW",
                "jobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
                "uploadDefinitionId": "38f47152-c3c2-471c-b7e0-c9d024e47357",
                "createDate": "2023-05-04T18:14:20.541+00:00",
            },
            {
                "id": "9b5cc830-0c3d-496a-8472-3b98aa40d108",
                "name": "0720230118_2.mrc",
                "status": "NEW",
                "jobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
                "uploadDefinitionId": "38f47152-c3c2-471c-b7e0-c9d024e47357",
                "createDate": "2023-05-04T18:14:20.541+00:00",
            },
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
                "sourcePath": "./storage/upload/38f47152-c3c2-471c-b7e0-c9d024e47357/8b5cc830-0c3d-496a-8472-3b98aa40d109/0720230118_1.mrc",
                "name": "0720230118_1.mrc",
                "status": "UPLOADED",
                "jobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
                "uploadDefinitionId": "38f47152-c3c2-471c-b7e0-c9d024e47357",
                "createDate": "2023-05-04T18:14:20.541+00:00",
                "uploadedDate": "2023-05-04T18:14:21.736+00:00",
            },
            {
                "id": "9b5cc830-0c3d-496a-8472-3b98aa40d108",
                "sourcePath": "./storage/upload/38f47152-c3c2-471c-b7e0-c9d024e47357/8b5cc830-0c3d-496a-8472-3b98aa40d109/0720230118_2.mrc",
                "name": "0720230118_2.mrc",
                "status": "UPLOADED",
                "jobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
                "uploadDefinitionId": "38f47152-c3c2-471c-b7e0-c9d024e47357",
                "createDate": "2023-05-04T18:14:20.541+00:00",
                "uploadedDate": "2023-05-04T18:14:21.736+00:00",
            },
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


FILENAMES = ["0720230118_1.mrc", "0720230118_2.mrc"]


@pytest.fixture
def download_path(tmp_path):
    for filename in FILENAMES:
        dest_filepath = os.path.join(tmp_path, filename)
        shutil.copyfile("tests/vendor/0720230118.mrc", dest_filepath)
    return str(tmp_path)


def test_data_import(download_path, folio_client):
    data_import(
        download_path,
        FILENAMES,
        "f4144dbd-def7-4b77-842a-954c62faf319",
        folio_client=folio_client,
    )

    folio_client.post.assert_any_call(
        "/data-import/uploadDefinitions",
        {
            "fileDefinitions": [
                {"name": "0720230118_1.mrc"},
                {"name": "0720230118_2.mrc"},
            ]
        },
    )
    folio_client.post_file.assert_any_call(
        "/data-import/uploadDefinitions/38f47152-c3c2-471c-b7e0-c9d024e47357/files/8b5cc830-0c3d-496a-8472-3b98aa40d109",
        os.path.join(download_path, "0720230118_1.mrc"),
    )
    folio_client.post_file.assert_called_with(
        "/data-import/uploadDefinitions/38f47152-c3c2-471c-b7e0-c9d024e47357/files/9b5cc830-0c3d-496a-8472-3b98aa40d108",
        os.path.join(download_path, "0720230118_2.mrc"),
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
                    "sourcePath": "./storage/upload/38f47152-c3c2-471c-b7e0-c9d024e47357/8b5cc830-0c3d-496a-8472-3b98aa40d109/0720230118_1.mrc",
                    "name": "0720230118_1.mrc",
                    "status": "UPLOADED",
                    "jobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
                    "uploadDefinitionId": "38f47152-c3c2-471c-b7e0-c9d024e47357",
                    "createDate": "2023-05-04T18:14:20.541+00:00",
                    "uploadedDate": "2023-05-04T18:14:21.736+00:00",
                },
                {
                    "id": "9b5cc830-0c3d-496a-8472-3b98aa40d108",
                    "sourcePath": "./storage/upload/38f47152-c3c2-471c-b7e0-c9d024e47357/8b5cc830-0c3d-496a-8472-3b98aa40d109/0720230118_2.mrc",
                    "name": "0720230118_2.mrc",
                    "status": "UPLOADED",
                    "jobExecutionId": "4a20579d-0a8f-4fed-8cf9-7c6d9f1fb2ae",
                    "uploadDefinitionId": "38f47152-c3c2-471c-b7e0-c9d024e47357",
                    "createDate": "2023-05-04T18:14:20.541+00:00",
                    "uploadedDate": "2023-05-04T18:14:21.736+00:00",
                },
            ],
            "metadata": {
                "createdDate": "2023-05-04T18:14:20.187+00:00",
                "createdByUserId": "3e2ed889-52f2-45ce-8a30-8767266f07d2",
                "updatedDate": "2023-05-04T18:14:20.187+00:00",
                "updatedByUserId": "3e2ed889-52f2-45ce-8a30-8767266f07d2",
            },
        },
        "jobProfileInfo": {
            "id": "f4144dbd-def7-4b77-842a-954c62faf319",
            "dataType": "MARC",
        },
    }

    folio_client.post.assert_called_with(
        "/data-import/uploadDefinitions/38f47152-c3c2-471c-b7e0-c9d024e47357/processFiles",
        process_files_payload,
    )


@pytest.fixture
def context():
    return {
        "params": {
            "vendor_interface_uuid": "65d30c15-a560-4064-be92-f90e38eeb351",
            "filename": "3820230411.mrc",
        }
    }


def test_record_loading(context, pg_hook):
    record_loading(context)

    with Session(pg_hook()) as session:
        vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "3820230411.mrc")
        ).first()
        assert vendor_file.status == FileStatus.loading


def test_record_loaded(context, pg_hook):
    record_loaded(context)

    with Session(pg_hook()) as session:
        vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "3820230411.mrc")
        ).first()
        assert vendor_file.status == FileStatus.loaded


def test_record_loading_error(context, pg_hook):
    record_loading_error(context)

    with Session(pg_hook()) as session:
        vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "3820230411.mrc")
        ).first()
        assert vendor_file.status == FileStatus.loading_error
