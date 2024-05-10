import pytest  # noqa
from pytest_mock_resources import create_sqlite_fixture, Rows

import shutil
from datetime import datetime, date

from libsys_airflow.plugins.vendor.archive import archive

from sqlalchemy.orm import Session
from sqlalchemy import select

from airflow.providers.postgres.hooks.postgres import PostgresHook
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
        last_folio_update=datetime.utcnow(),
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
        created=datetime.utcnow(),
        updated=datetime.utcnow(),
        vendor_interface_id=1,
        vendor_filename="0720230118.mrc",
        filesize=123,
        status=FileStatus.fetched,
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
def download_path(tmp_path):
    path = tmp_path / "download"
    path.mkdir(parents=True, exist_ok=True)
    shutil.copyfile("tests/vendor/0720230118.mrc", path / "0720230118.mrc")
    return path


@pytest.fixture
def archive_path(tmp_path):
    path = tmp_path / "archive"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_archive_no_files(download_path, archive_path, pg_hook):
    with Session(pg_hook()) as session:
        archive(
            [],
            download_path,
            '698a62fe-8aff-40c7-b1ef-e8bd13c77536',
            '65d30c15-a560-4064-be92-f90e38eeb351',
            session,
        )

    assert [r for r in archive_path.iterdir()] == []


def test_archive(download_path, archive_path, pg_hook, mocker):
    mocker.patch(
        'libsys_airflow.plugins.vendor.paths.archive_basepath',
        return_value=archive_path,
    )

    with Session(pg_hook()) as session:
        archive(
            ["0720230118.mrc"],
            download_path,
            '698a62fe-8aff-40c7-b1ef-e8bd13c77536',
            '65d30c15-a560-4064-be92-f90e38eeb351',
            session,
        )

    vendor_interface_archived_path = (
        archive_path
        / f"{date.today().strftime('%Y%m%d')}/698a62fe-8aff-40c7-b1ef-e8bd13c77536/65d30c15-a560-4064-be92-f90e38eeb351"
    )
    archived_file = next(vendor_interface_archived_path.glob("*.mrc"))
    assert archived_file.name == "0720230118.mrc"

    with Session(pg_hook()) as session:
        vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "0720230118.mrc")
        ).first()
        assert vendor_file.archive_date == date.today()
