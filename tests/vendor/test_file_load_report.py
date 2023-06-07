import pytest  # noqa
from pytest_mock_resources import create_sqlite_fixture, Rows
from unittest.mock import Mock
from mocks import mock_dag_run  # noqa: F401

from datetime import datetime

from sqlalchemy.orm import Session

from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.vendor.file_load_report import report_when_file_loaded
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
        dag_run_id="manual_2022-03-05",  # NOTE: value from mock_dag_run in mocks.py
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
def folio_client():
    job_summary_resp = {
        'jobExecutionId': 'd7460945-6f0c-4e74-86c9-34a8438d652e',
        'totalErrors': 0,
        'sourceRecordSummary': {
            'totalCreatedEntities': 2,
            'totalUpdatedEntities': 0,
            'totalDiscardedEntities': 0,
            'totalErrors': 0,
        },
        'instanceSummary': {
            'totalCreatedEntities': 2,
            'totalUpdatedEntities': 0,
            'totalDiscardedEntities': 0,
            'totalErrors': 0,
        },
    }
    mock_client = Mock()
    mock_client.get.return_value = job_summary_resp
    return mock_client


def test_file_load_report_with_loaded_file(
    pg_hook, mocker, mock_dag_run, folio_client  # noqa: F811
):
    with Session(pg_hook()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        mocker.patch('airflow.models.DagRun.find', return_value=[mock_dag_run])
        return_value = report_when_file_loaded(
            "65d30c15-a560-4064-be92-f90e38eeb351",
            "loaded.mrc",
            client=folio_client,
        )
        assert return_value.is_done is True


def test_file_load_report_with_no_file(pg_hook, mocker):
    with Session(pg_hook()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        return_value = report_when_file_loaded(
            "65d30c15-a560-4064-be92-f90e38eeb351",
            "nonexistent.mrc",
            client=folio_client,
        )
        assert return_value.is_done is False


def test_file_load_report_with_not_loaded_file(pg_hook, mocker):
    with Session(pg_hook()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        return_value = report_when_file_loaded(
            "65d30c15-a560-4064-be92-f90e38eeb351",
            "not_loaded.mrc",
            client=folio_client,
        )
        assert return_value.is_done is False


def test_file_load_report_with_file_lacking_job_execution_uuid(pg_hook, mocker):
    with Session(pg_hook()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        return_value = report_when_file_loaded(
            "65d30c15-a560-4064-be92-f90e38eeb351",
            "loaded_but_messed_up.mrc",
            client=folio_client,
        )
        assert return_value.is_done is False
