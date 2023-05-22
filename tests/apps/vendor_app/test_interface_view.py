import os
from datetime import datetime, timedelta

import pytest
from pytest_mock_resources import create_sqlite_fixture, Rows
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from libsys_airflow.plugins.vendor.models import (
    Vendor,
    VendorInterface,
    VendorFile,
    FileStatus,
)
from tests.airflow_client import test_airflow_client  # noqa: F401

load_dotenv()
now = datetime.now()

rows = Rows(
    # set up a vendor and its "interface"
    Vendor(
        id=1,
        display_name="Acme",
        folio_organization_uuid="375C6E33-2468-40BD-A5F2-73F82FE56DB0",
        vendor_code_from_folio="ACME",
        acquisitions_unit_from_folio="ACMEUNIT",
        has_active_vendor_interfaces=False,
        last_folio_update=now,
    ),
    VendorInterface(
        id=1,
        display_name="Acme FTP",
        vendor_id=1,
        folio_interface_uuid="140530EB-EE54-4302-81EE-D83B9DAC9B6E",
        folio_data_import_processing_name="Acme Profile 1",
        file_pattern="*.mrc",
        remote_path="stanford/outgoing/data",
        processing_dag="acme-pull",
        processing_delay_in_days=3,
        active=True,
    ),
    # a file was fetched 10 days ago, and was loaded 9 days ago
    VendorFile(
        id=1,
        created=now - timedelta(days=10),
        updated=now - timedelta(days=10),
        vendor_interface_id=1,
        vendor_filename="acme-marc.dat",
        filesize=1234,
        vendor_timestamp=now - timedelta(days=30),
        loaded_timestamp=now - timedelta(days=8),
        expected_execution=now - timedelta(days=9),
        status=FileStatus.loaded,
        dag_run_id="EB2FD649-2B0A-4671-9BD4-309BF5197870",
    ),
    # a file was fetched 8 days ago but errored during load 3 days ago
    VendorFile(
        id=2,
        created=now - timedelta(days=8),
        updated=now - timedelta(days=8),
        vendor_interface_id=1,
        vendor_filename="acme-bad-marc.dat",
        filesize=123456,
        vendor_timestamp=now - timedelta(days=30),
        loaded_timestamp=None,
        expected_execution=now - timedelta(days=3),
        status=FileStatus.loading_error,
        dag_run_id="520F69D8-97BF-47CA-976A-F9CD4DB3FAD7",
    ),
    # a file was fetched .5 days ago, and is waiting to be loaded
    VendorFile(
        id=3,
        created=now - timedelta(days=0.5),
        updated=now - timedelta(days=0.5),
        vendor_interface_id=1,
        vendor_filename="acme-extra-strength-marc.dat",
        filesize=1234567,
        vendor_timestamp=now - timedelta(days=12),
        loaded_timestamp=None,
        expected_execution=now + timedelta(days=1),
        status=FileStatus.fetched,
    ),
    # a file was fetched 1 day ago, and is waiting to be loaded
    VendorFile(
        id=4,
        created=now - timedelta(days=1),
        updated=now - timedelta(days=1),
        vendor_interface_id=1,
        vendor_filename="acme-lite-marc.dat",
        filesize=1234567,
        vendor_timestamp=now - timedelta(days=14),
        loaded_timestamp=None,
        expected_execution=now + timedelta(days=0.5),
        status=FileStatus.fetched,
    ),
    # a file was that failed to fetch right now
    VendorFile(
        id=5,
        created=now - timedelta(days=1),
        updated=now,
        vendor_interface_id=1,
        vendor_filename="acme-ftp-broken-marc.dat",
        filesize=24601,
        vendor_timestamp=now - timedelta(days=14),
        loaded_timestamp=None,
        expected_execution=now,
        status=FileStatus.fetching_error,
    ),
)

engine = create_sqlite_fixture(rows)


@pytest.fixture
def mock_db(mocker, engine):
    mock_hook = mocker.patch(
        "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
    )
    mock_hook.return_value = engine
    yield mock_hook


def test_vendor_files(mock_db):  # noqa: F811
    session = Session(mock_db())
    interface = session.get(VendorInterface, 1)
    assert len(interface.vendor_files) == 5


def test_pending_files(mock_db):
    session = Session(mock_db())
    interface = session.get(VendorInterface, 1)
    pending = interface.pending_files
    assert len(pending) == 3
    assert [v.vendor_filename for v in pending] == [
        "acme-extra-strength-marc.dat",
        "acme-lite-marc.dat",
        "acme-ftp-broken-marc.dat",
    ]


def test_processed_files(mock_db):
    session = Session(mock_db())
    interface = session.get(VendorInterface, 1)
    processed = interface.processed_files
    assert len(processed) == 2
    assert [v.vendor_filename for v in processed] == [
        "acme-marc.dat",
        "acme-bad-marc.dat",
    ]


def test_interface_view(test_airflow_client, mock_db):  # noqa: F811
    response = test_airflow_client.get('/vendors/interface/1')
    assert response.status_code == 200
    pending = response.html.find(id='pending-files')
    assert pending
    assert len(pending.find_all('tr')) == 3

    loaded = response.html.find(id='loaded-files')
    assert loaded
    assert len(loaded.find_all('tr')) == 2


@pytest.mark.skipif(
    os.environ.get("AIRFLOW_VAR_OKAPI_URL") is None, reason="No Folio Environment"
)
def test_interface_edit_view(test_airflow_client, mock_db):  # noqa: F811
    response = test_airflow_client.get('/vendors/interface/1/edit')
    assert response.status_code == 200
