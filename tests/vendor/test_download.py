import pytest  # noqa
import pathlib
from datetime import datetime
from pytest_mock_resources import create_sqlite_fixture, Rows


from libsys_airflow.plugins.vendor.download import (
    download,
    _regex_filter_strategy,
    _gobi_order_filter_strategy,
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
        filesize=123,
        status=FileStatus.not_fetched,
        vendor_timestamp=datetime.fromisoformat("2022-01-01T00:05:23"),
        expected_execution=datetime.now().date(),
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
def ftp_hook(mocker):
    mock_hook = mocker.patch("airflow.providers.ftp.hooks.ftp.FTPHook")
    mock_hook.list_directory.return_value = [
        "3820230411.mrc",
        "3820230412.mrc",
        "3820230412.xxx",
    ]
    mock_hook.get_size.return_value = 123
    mock_hook.get_mod_time.return_value = datetime.fromisoformat("2023-01-01T00:05:23")
    return mock_hook


@pytest.fixture
def download_path(tmp_path):
    pathlib.Path(f"{tmp_path}/3820230412.mrc").touch()
    return str(tmp_path)


def test_download(ftp_hook, download_path, pg_hook):
    download(
        ftp_hook,
        "oclc",
        download_path,
        _regex_filter_strategy(r".+\.mrc"),
        "65d30c15-a560-4064-be92-f90e38eeb351",
        "2023-01-01T00:05:23",
    )

    assert ftp_hook.list_directory.call_count == 1
    assert ftp_hook.list_directory.called_with("oclc")
    assert ftp_hook.get_size.call_count == 1
    assert ftp_hook.get_size.called_with("3820230411.mrc")
    assert ftp_hook.get_mod_time.call_count == 1
    assert ftp_hook.get_mod_time.called_with("3820230411.mrc")
    assert ftp_hook.retrieve_file.call_count == 1
    assert ftp_hook.retrieve_file.called_with(
        "3820230411.mrc", f"{download_path}/3820230411.mrc"
    )

    with Session(pg_hook()) as session:
        vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "3820230411.mrc")
        ).first()
        assert vendor_file.vendor_interface_id == 1
        assert vendor_file.filesize == 123
        assert vendor_file.status == FileStatus.fetched
        assert vendor_file.vendor_timestamp == datetime.fromisoformat(
            "2023-01-01T00:05:23"
        )


def test_download_error(ftp_hook, download_path, pg_hook):
    ftp_hook.retrieve_file.side_effect = Exception("Error")

    with pytest.raises(Exception):
        download(
            ftp_hook,
            "oclc",
            download_path,
            _regex_filter_strategy(r".+\.mrc"),
            "65d30c15-a560-4064-be92-f90e38eeb351",
            "2023-01-01T00:05:23",
        )

    with Session(pg_hook()) as session:
        vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "3820230411.mrc")
        ).first()
        assert vendor_file.vendor_interface_id == 1
        assert vendor_file.filesize == 123
        assert vendor_file.status == FileStatus.fetching_error
        assert vendor_file.vendor_timestamp == datetime.fromisoformat(
            "2023-01-01T00:05:23"
        )


def test_download_gobi_order(ftp_hook, download_path, pg_hook):
    ftp_hook.list_directory.return_value = [
        "3820230411.ord",
        "3820230411.cnt",
        "3820230412.ord",
        "3820230413.cnt",
    ]

    download(
        ftp_hook,
        "orders",
        download_path,
        _gobi_order_filter_strategy(),
        "65d30c15-a560-4064-be92-f90e38eeb351",
        "2023-01-01T00:05:23",
    )

    assert ftp_hook.list_directory.call_count == 1
    assert ftp_hook.list_directory.called_with("orders")
    assert ftp_hook.get_size.call_count == 1
    assert ftp_hook.get_size.called_with("3820230411.ord")
    assert ftp_hook.get_mod_time.call_count == 1
    assert ftp_hook.get_mod_time.called_with("3820230411.ord")
    assert ftp_hook.retrieve_file.call_count == 1
    assert ftp_hook.retrieve_file.called_with(
        "3820230411.ord", f"{download_path}/3820230411.ord"
    )
