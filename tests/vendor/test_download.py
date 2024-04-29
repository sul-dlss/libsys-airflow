import ftplib
import pytest  # noqa
from datetime import datetime
from pytest_mock_resources import create_sqlite_fixture, Rows


from libsys_airflow.plugins.vendor.download import (
    download,
    FTPAdapter,
    _download_filepath,
    _regex_filter_strategy,
    _gobi_order_filter_strategy,
)
from libsys_airflow.plugins.vendor.models import VendorInterface, VendorFile, FileStatus

from sqlalchemy.orm import Session
from sqlalchemy import select

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sftp.hooks.sftp import SFTPHook


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
        created=datetime.utcnow(),
        updated=datetime.utcnow(),
        vendor_interface_id=1,
        vendor_filename="3820230411.mrc",
        filesize=123,
        status=FileStatus.not_fetched,
        vendor_timestamp=datetime.fromisoformat("2022-01-01T00:05:23"),
    ),
    VendorFile(
        created=datetime.utcnow(),
        updated=datetime.utcnow(),
        vendor_interface_id=1,
        vendor_filename="3820230412.mrc",
        filesize=456,
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
def ftp_hook(mocker):
    mock_hook = mocker.patch("airflow.providers.ftp.hooks.ftp.FTPHook")
    mock_hook.list_directory.return_value = [
        "3820230411.mrc",  # Downloaded
        "3820230412.mrc",  # Already fetched
        "3820230413.mrc",  # Outside download window
        "3820230412.xxx",  # Does not match regex
    ]
    mock_hook.get_size.side_effect = [123, 678]
    mock_hook.get_mod_time.side_effect = [
        datetime.fromisoformat("2023-01-01T00:05:23"),
        datetime.fromisoformat("2013-01-01T00:05:23"),
        datetime.fromisoformat("2023-01-01T00:05:23"),
    ]
    return mock_hook


@pytest.fixture
def sftp_hook(mocker):
    mock_hook = mocker.patch("airflow.providers.sftp.hooks.sftp.SFTPHook")
    mock_hook.describe_directory.return_value = {
        "3820230411.mrc": {"size": 123, "modify": "20230101000523"},
        "3820230412.mrc": {"size": 456, "modify": "20230101000523"},
        "3820230413.mrc": {"size": 678, "modify": "20130101000523"},
        "3820230412.xxx": {},
    }
    mock_hook.__class__ = SFTPHook
    return mock_hook


@pytest.fixture
def download_path(tmp_path):
    return str(tmp_path)


def test_ftp_download(ftp_hook, download_path, pg_hook):
    download(
        ftp_hook,
        "oclc",
        download_path,
        _regex_filter_strategy(r".+\.mrc"),
        "65d30c15-a560-4064-be92-f90e38eeb351",
        datetime.fromisoformat("2020-01-01T00:05:23"),
    )

    assert ftp_hook.list_directory.call_count == 1
    assert ftp_hook.list_directory.called_with("oclc")
    assert ftp_hook.get_size.call_count == 2
    assert ftp_hook.get_size.called_with("3820230411.mrc")
    assert ftp_hook.get_mod_time.call_count == 3
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
        assert vendor_file.filesize == 678
        assert vendor_file.status == FileStatus.fetched
        assert vendor_file.vendor_timestamp == datetime.fromisoformat(
            "2023-01-01T00:05:23"
        )
        skipped_vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "3820230413.mrc")
        ).first()
        assert skipped_vendor_file.status == FileStatus.skipped


def mock_ftp_hook_with_errors(mocker):

    def mock_get_mod_time(*args):
        if args[0] == "0820240402_f.mrc":
            raise ftplib.error_perm("550 The system cannot find the file specified.")
        elif args[0].startswith("oclc"):
            return "20240411193159"

    def mock_get_size(*args):
        if args[0] == "1120240402_f.mrc":
            raise ftplib.error_perm("550 The system cannot find the file specified.")
        elif args[0].startswith("oclc"):
            return "563"

    def mock_list_directory(*args):
        return ['0820240402_f.mrc', '1120240402_f.mrc', '1220240402_f.mrc']

    def mock_retrieve_file(*args):
        if args[0] == "1220240402_f.mrc":
            raise ftplib.error_perm("550 The system cannot find the file specified.")

    mock_ftp_hook = mocker.MagicMock()
    mock_ftp_hook.get_mod_time = mock_get_mod_time
    mock_ftp_hook.get_size = mock_get_size
    mock_ftp_hook.list_directory = mock_list_directory
    mock_ftp_hook.retrieve_file = mock_retrieve_file
    return mock_ftp_hook


def test_ftp_download_error_handling(mocker, caplog):

    adapter = FTPAdapter(hook=mock_ftp_hook_with_errors(mocker), remote_path="oclc")

    mod_time = adapter.get_mod_time('0820240402_f.mrc')

    assert "Failed to retrieve modified time" in caplog.text
    assert mod_time == "20240411193159"

    file_size = adapter.get_size('1120240402_f.mrc')

    assert "Failed to retrieve size" in caplog.text
    assert file_size == "563"

    adapter.retrieve_file("1220240402_f.mrc", "/home/airflow/vendor-files/abded-abcd")

    assert "Failed to retrieve 1220240402_f.mrc" in caplog.text


def test_sftp_download(sftp_hook, download_path, pg_hook):
    download(
        sftp_hook,
        "oclc",
        download_path,
        _regex_filter_strategy(r".+\.mrc"),
        "65d30c15-a560-4064-be92-f90e38eeb351",
        datetime.fromisoformat("2020-01-01T00:05:23"),
    )

    assert sftp_hook.describe_directory.call_count == 1
    assert sftp_hook.describe_directory.called_with("oclc")
    assert sftp_hook.retrieve_file.call_count == 1
    assert sftp_hook.retrieve_file.called_with(
        "oclc/3820230411.mrc", f"{download_path}/3820230411.mrc"
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
            datetime.fromisoformat("2020-01-01T00:05:23"),
        )

    with Session(pg_hook()) as session:
        vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "3820230411.mrc")
        ).first()
        assert vendor_file.vendor_interface_id == 1
        assert vendor_file.filesize == 678
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
        datetime.fromisoformat("2020-01-01T00:05:23"),
    )

    assert ftp_hook.list_directory.call_count == 1
    assert ftp_hook.list_directory.called_with("orders")
    assert ftp_hook.get_size.call_count == 1
    assert ftp_hook.get_size.called_with("3820230411.ord")
    assert ftp_hook.get_mod_time.call_count == 2
    assert ftp_hook.get_mod_time.called_with("3820230411.ord")
    assert ftp_hook.retrieve_file.call_count == 1
    assert ftp_hook.retrieve_file.called_with(
        "3820230411.ord", f"{download_path}/3820230411.ord"
    )


def test_missing_parent_directories(tmp_path):
    download_filepath = tmp_path / "Stanford/test.mrc"

    assert not download_filepath.parent.exists()

    _download_filepath(tmp_path, "Stanford/test.mrc")

    assert download_filepath.parent.exists()
