import pytest  # noqa
from pytest_mock_resources import create_sqlite_fixture, Rows
from unittest.mock import Mock

from datetime import datetime

from sqlalchemy.orm import Session

from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.vendor.file_loaded_sensor import file_loaded_sensor
from libsys_airflow.plugins.vendor.models import (
    VendorInterface,
    VendorFile,
    FileStatus,
    Vendor,
)


rows = Rows(
    Vendor(
        id=1,
        display_name="Acme",
        folio_organization_uuid="698a62fe-8aff-40c7-b1ef-e8bd13c77536",
        vendor_code_from_folio="Gobi",
        acquisitions_unit_from_folio="ACMEUNIT",
        last_folio_update=datetime.now(),
    ),
    VendorInterface(
        id=1,
        display_name="Gobi - Full bibs",
        vendor_id=1,
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
        vendor_filename="not_loaded.mrc",
        filesize=123,
        status=FileStatus.fetched,
        vendor_timestamp=datetime.fromisoformat("2022-01-01T00:05:23"),
    ),
    VendorFile(
        created=datetime.now(),
        updated=datetime.now(),
        vendor_interface_id=1,
        vendor_filename="loaded.mrc",
        dag_run_id="manual_2022-03-05",
        filesize=234,
        folio_job_execution_uuid="d7460945-6f0c-4e74-86c9-34a8438d652e",
        status=FileStatus.loaded,
        vendor_timestamp=datetime.fromisoformat("2022-01-01T00:05:23"),
    ),
    VendorFile(
        created=datetime.now(),
        updated=datetime.now(),
        vendor_interface_id=1,
        vendor_filename="loaded_but_messed_up.mrc",
        filesize=345,
        status=FileStatus.loaded,
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
def folio_client(request):
    job_execution_resp = {
        "id": "d7460945-6f0c-4e74-86c9-34a8438d652e",
        "hrId": 10000,
        "jobProfileInfo": {
            "id": "88dfac11-1caf-4470-9ad1-d533f6360bad",
            "name": "stub job profile",
            "dataType": "MARC",
        },
        "jobProfileSnapshotWrapper": {
            "id": "0fc0ba76-d560-4541-826f-f8bdd3b09841",
            "profileId": "dc4ac439-57ae-4779-872e-1892b163bb47",
            "contentType": "JOB_PROFILE",
            "reactTo": "NON-MATCH",
            "content": {
                "id": "dc4ac439-57ae-4779-872e-1892b163bb47",
                "name": "Load vendor order records",
                "description": "Ordered on vendor site; load MARC to create bib, holdings, item, and order; keep MARC",
                "dataTypes": ["MARC_BIB"],
                "tags": {"tagList": ["acq", "daily"]},
            },
        },
        "parentJobId": "67dfac11-1caf-4470-9ad1-d533f6360bdd",
        "subordinationType": "PARENT_SINGLE",
        "sourcePath": "mport_1.csv",
        "fileName": "import_1.csv",
        "runBy": {"firstName": "DIKU", "lastName": "ADMINISTRATOR"},
        "progress": {
            "jobExecutionId": "67dfac11-1caf-4470-9ad1-d533f6360bdd",
            "current": 50,
            "total": 50,
        },
        "startedDate": "2018-10-30T12:36:55.000",
        "completedDate": "2018-10-30T12:40:01.000",
        "status": request.param,
        "uiStatus": "RUNNING_COMPLETE",
        "userId": "c9db5d7a-e1d4-11e8-9f32-f2801f1b9fd1",
    }
    mock_client = Mock()
    mock_client.folio_get.return_value = job_execution_resp
    return mock_client


@pytest.mark.parametrize('folio_client', ['COMMITTED'], indirect=True)
def test_file_load_report_with_loaded_file_and_job_committed(
    pg_hook,
    mocker,
    folio_client,  # noqa: F811
):
    with Session(pg_hook()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        return_value = file_loaded_sensor(
            "65d30c15-a560-4064-be92-f90e38eeb351",
            "loaded.mrc",
            "d7460945-6f0c-4e74-86c9-34a8438d652e",
            client=folio_client,
        )
        assert return_value.is_done is True


@pytest.mark.parametrize('folio_client', ['FILE_UPLOADED'], indirect=True)
def test_file_load_report_with_loaded_file_and_job_not_committed(
    pg_hook, mocker, folio_client  # noqa: F811
):
    with Session(pg_hook()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        return_value = file_loaded_sensor(
            "65d30c15-a560-4064-be92-f90e38eeb351",
            "loaded.mrc",
            "d7460945-6f0c-4e74-86c9-34a8438d652e",
            client=folio_client,
        )
        assert return_value.is_done is False


def test_file_load_report_with_no_file(pg_hook, mocker):
    with Session(pg_hook()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        return_value = file_loaded_sensor(
            "65d30c15-a560-4064-be92-f90e38eeb351",
            "nonexistent.mrc",
            "d7460945-6f0c-4e74-86c9-34a8438d652e",
            client=folio_client,
        )
        assert return_value.is_done is False


def test_file_load_report_with_not_loaded_file(pg_hook, mocker):
    with Session(pg_hook()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        return_value = file_loaded_sensor(
            "65d30c15-a560-4064-be92-f90e38eeb351",
            "not_loaded.mrc",
            "d7460945-6f0c-4e74-86c9-34a8438d652e",
            client=folio_client,
        )
        assert return_value.is_done is False


def test_file_load_report_with_file_lacking_job_execution_uuid(pg_hook, mocker):
    with Session(pg_hook()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        return_value = file_loaded_sensor(
            "65d30c15-a560-4064-be92-f90e38eeb351",
            "loaded_but_messed_up.mrc",
            "d7460945-6f0c-4e74-86c9-34a8438d652e",
            client=folio_client,
        )
        assert return_value.is_done is False
