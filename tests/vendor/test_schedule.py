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
        has_active_vendor_interfaces=False,
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
        vendor_filename="waiting.dat",
        filesize=1234567,
        vendor_timestamp=now - timedelta(days=14),
        loaded_timestamp=None,
        expected_load_time=now + timedelta(days=2),
        status=FileStatus.fetched,
    ),
    # a file that was fetched 2 days ago and was ready for loading 10 minutes ago
    VendorFile(
        id=2,
        created=now - timedelta(days=1),
        updated=now - timedelta(days=1),
        vendor_interface_id=1,
        vendor_filename="ready.dat",
        filesize=1234567,
        vendor_timestamp=now - timedelta(days=14),
        loaded_timestamp=None,
        expected_load_time=now - timedelta(minutes=10),
        status=FileStatus.fetched,
    ),
    # a file that was fetched 2 days ago and was ready for loading 8 hours ago
    VendorFile(
        id=3,
        created=now - timedelta(days=2),
        updated=now - timedelta(days=1),
        vendor_interface_id=1,
        vendor_filename="ready-too.dat",
        filesize=1234567,
        vendor_timestamp=now - timedelta(days=14),
        loaded_timestamp=None,
        expected_load_time=now - timedelta(hours=8),
        status=FileStatus.fetched,
    ),
    # a file that was fetched 2 days ago and has been loaded
    VendorFile(
        id=4,
        created=now - timedelta(days=1),
        updated=now - timedelta(days=1),
        vendor_interface_id=1,
        vendor_filename="loaded.dat",
        filesize=1234567,
        vendor_timestamp=now - timedelta(days=14),
        loaded_timestamp=None,
        expected_load_time=now - timedelta(hours=8),
        status=FileStatus.loaded,
    ),
    # a file that was fetched 2 days ago, was loaded but resulted in an error
    VendorFile(
        id=5,
        created=now - timedelta(days=1),
        updated=now - timedelta(days=1),
        vendor_interface_id=1,
        vendor_filename="error.dat",
        filesize=1234567,
        vendor_timestamp=now - timedelta(days=14),
        loaded_timestamp=None,
        expected_load_time=now - timedelta(hours=8),
        status=FileStatus.loading_error,
    ),
    # A vendor interface with now import profile (which should be skipped)
    VendorInterface(
        id=2,
        display_name="Acme FTP No Import Profile",
        vendor_id=1,
        folio_interface_uuid=None,
        folio_data_import_processing_name=None,
        folio_data_import_profile_uuid=None,
        file_pattern="*.mrc",
        remote_path="stanford/outgoing/data",
        processing_dag="acme-pull",
        processing_delay_in_days=3,
        active=True,
    ),
    # a ready file but which is attached to an interface without an import profile
    # and which consequently has no expected_load_time
    VendorFile(
        id=6,
        created=now - timedelta(days=1),
        updated=now - timedelta(days=1),
        vendor_interface_id=2,
        vendor_filename="no-profile.dat",
        filesize=1234567,
        vendor_timestamp=now - timedelta(days=14),
        loaded_timestamp=None,
        expected_load_time=None,
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


def test_schedule(engine):
    with Session(engine) as session:
        vendor_files = VendorFile.ready_for_data_import(session)
        # only "fetched" fils with expected_load_time in the past should be returned
        assert len(vendor_files) == 2
        # results should be in ascending order by when they were fetched
        assert vendor_files[0].created < vendor_files[1].created
