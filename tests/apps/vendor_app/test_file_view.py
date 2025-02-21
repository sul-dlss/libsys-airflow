from datetime import datetime, timedelta

import pytest
from pytest_mock_resources import create_sqlite_fixture, Rows
from sqlalchemy.orm import Session

from libsys_airflow.plugins.vendor.models import (
    Vendor,
    VendorInterface,
    VendorFile,
    FileStatus,
)
from tests.airflow_client import test_airflow_client  # noqa: F401

now = datetime.utcnow()

rows = Rows(
    # set up a vendor and its "interface"
    Vendor(
        id=1,
        display_name="Acme",
        folio_organization_uuid="375C6E33-2468-40BD-A5F2-73F82FE56DB0",
        vendor_code_from_folio="ACME",
        acquisitions_unit_from_folio="ACMEUNIT",
        last_folio_update=now,
    ),
    VendorInterface(
        id=1,
        display_name="Acme FTP",
        vendor_id=1,
        folio_interface_uuid="140530EB-EE54-4302-81EE-D83B9DAC9B6E",
        folio_data_import_processing_name="Acme Profile 1",
        folio_data_import_profile_uuid="A8635200-F876-46E0-ACF0-8E0EFA542A3F",
        file_pattern="*.mrc",
        remote_path="stanford/outgoing/data",
        processing_dag="acme-pull",
        processing_delay_in_days=3,
        active=True,
    ),
    # a file was fetched 1 day ago, and is waiting to be loaded in 2 days
    VendorFile(
        id=1,
        created=now - timedelta(days=1),
        updated=now - timedelta(days=1),
        vendor_interface_id=1,
        vendor_filename="acme-lite-marc.dat",
        filesize=1234567,
        vendor_timestamp=now - timedelta(days=14),
        loaded_timestamp=None,
        expected_processing_time=now + timedelta(days=2),
        status=FileStatus.fetched,
    ),
)

engine = create_sqlite_fixture(rows)


@pytest.fixture
def mock_db(mocker, engine):
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    return mock_hook


@pytest.fixture
def mock_variable(mocker):
    return mocker.patch(
        'libsys_airflow.plugins.vendor_app.vendor_management.Variable.get',
        return_value='https://folio-stage.edu/',
    )


def test_file_view(test_airflow_client, mock_db, mock_variable, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        response = test_airflow_client.get('/vendor_management/files/1')
        assert response.status_code == 200

        expected_processing_time = response.html.select_one(
            '#expected-processing-time input'
        )
        assert expected_processing_time
        expected_processing_time = expected_processing_time.attrs['value']
        expected_processing_time = datetime.fromisoformat(expected_processing_time)
        # the <input type=timelocal> doesn't do microseconds
        then = (now + timedelta(days=2)).replace(microsecond=0)
        assert expected_processing_time.date() == then.date()
        assert expected_processing_time.time() == then.time()


def test_missing_file(test_airflow_client, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )
        response = test_airflow_client.get('/vendor_management/files/1990')
        assert response.status_code == 404


def test_expected_processing_time(
    test_airflow_client, mock_variable, mock_db, mocker  # noqa: F811
):
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )

        # update the expected_processing_time with a POST
        # the <input type=timelocal> doesn't do microseconds
        tomorrow = (now + timedelta(days=1)).replace(microsecond=0)
        response = test_airflow_client.post(
            '/vendor_management/files/1',
            data={'expected-processing-time': tomorrow.isoformat()},
        )
        assert response.status_code == 200

        # ensure HTML response includes the updated expected-processing-time
        expected_processing_time = response.html.select_one(
            '#expected-processing-time input'
        )
        assert expected_processing_time
        expected_processing_time = expected_processing_time.attrs['value']
        expected_processing_time = datetime.fromisoformat(expected_processing_time)
        assert expected_processing_time.date() == tomorrow.date()
        assert expected_processing_time.time() == tomorrow.time().replace(microsecond=0)

        # peek in the database to see if it was updated there
        vendor_file = session.get(VendorFile, 1)
        assert vendor_file.expected_processing_time.date() == tomorrow.date()
        assert vendor_file.expected_processing_time.time() == tomorrow.time()


def test_set_status(test_airflow_client, mock_variable, mock_db, mocker):  # noqa: F811
    with Session(mock_db()) as session:
        mocker.patch(
            'libsys_airflow.plugins.vendor_app.vendor_management.Session',
            return_value=session,
        )

        # update the expected_processing_time with a POST
        # the <input type=timelocal> doesn't do microseconds
        response = test_airflow_client.post(
            '/vendor_management/files/1',
            data={'status': FileStatus.loaded.value},
        )
        assert response.status_code == 200

        # ensure that the ability to set status with the select input is now removed
        status_input = response.html.select_one('#status select')
        assert status_input is None

        # peek in the database to see if it was updated there
        vendor_file = session.get(VendorFile, 1)
        assert vendor_file.status == FileStatus.loaded
        assert len(vendor_file.loaded_history) == 1
