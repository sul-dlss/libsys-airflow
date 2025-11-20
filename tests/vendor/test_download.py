import ftplib  # noqa
import pytest  # noqa
from datetime import datetime, timezone, timedelta
from pytest_mock_resources import create_sqlite_fixture, Rows


from libsys_airflow.plugins.vendor.download import (
    FTPAdapter,
    SFTPAdapter,
    filter_by_strategy,
    filter_already_downloaded,
    filter_by_mod_date,
    download_task,
    update_vendor_files_table,
    _filter_remote_path,
)
from libsys_airflow.plugins.vendor.models import (
    Vendor,
    VendorInterface,
    VendorFile,
    FileStatus,
)

from sqlalchemy.orm import Session
from sqlalchemy import select

from airflow.providers.postgres.hooks.postgres import PostgresHook


rows = Rows(
    Vendor(
        id=1,
        display_name="Gobi",
        folio_organization_uuid="43459f05-f98b-43c0-a79d-76a8855dba94",
        vendor_code_from_folio="GOBI",
        last_folio_update=datetime.fromisoformat("2024-05-09T00:05:23"),
    ),
    VendorInterface(
        id=1,
        display_name="Gobi - Full bibs",
        folio_interface_uuid="65d30c15-a560-4064-be92-f90e38eeb351",
        folio_data_import_profile_uuid="f4144dbd-def7-4b77-842a-954c62faf319",
        file_pattern=r"^\d+\.mrc$",
        vendor_id=1,
        remote_path="oclc",
        active=True,
    ),
    VendorFile(
        created=datetime.now(timezone.utc),
        updated=datetime.now(timezone.utc),
        vendor_interface_id=1,
        vendor_filename="3820230411.mrc",
        filesize=123,
        status=FileStatus.not_fetched,
        vendor_timestamp=datetime.fromisoformat("2022-01-01T00:05:23"),
    ),
    VendorFile(
        created=datetime.now(timezone.utc),
        updated=datetime.now(timezone.utc),
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
def download_path(tmp_path):
    return str(tmp_path)


@pytest.fixture(params=["ftp_error", "ftp_download", "sftp_download", "gobi"])
def mock_hook(mocker, request):
    test_type = request.param

    def mock_get_mod_time(*args):
        if args[0] == "0820240402_f.mrc" and test_type == "ftp_error":
            raise ftplib.error_perm("550 The system cannot find the file specified.")
        elif args[0].startswith("oclc") and test_type == "ftp_error":
            return datetime.fromisoformat("2024-04-11T19:31:59")
        elif args[0] == "3820230411.mrc" and test_type == "ftp_download":  # Downloaded
            five_days_ago = (
                datetime.now(timezone.utc) - timedelta(days=int(5))
            ).isoformat(timespec="seconds")
            return datetime.fromisoformat(five_days_ago)
        elif (
            args[0] == "3820230413.mrc" and test_type == "ftp_download"
        ):  # Outside download window
            return datetime.fromisoformat("2013-01-01T00:05:23")
        elif test_type == "sftp_download":
            return "blah"

    def mock_get_size(*args):
        if args[0] == "1120240402_f.mrc":
            raise ftplib.error_perm("550 The system cannot find the file specified.")
        elif args[0].startswith("oclc"):
            return 563
        elif args[0] == "3820230411.mrc":
            return 123
        elif args[0] == "3820230413.mrc":
            return 678

    def mock_list_directory(*args):
        if test_type == "ftp_error":
            return ['0820240402_f.mrc', '1120240402_f.mrc', '1220240402_f.mrc']
        elif test_type == "ftp_download":
            return [
                "3820230411.mrc",  # Downloaded
                "3820230412.mrc",  # Already fetched
                "3820230413.mrc",  # Outside download window
                "3820230412.xxx",  # Does not match regex
            ]
        elif test_type == "sftp_download":
            return {
                "3820230411.mrc": {"size": 123, "modify": "20230101000523"},
                "3820230412.mrc": {"size": 456, "modify": "20230101000523"},
                "3820230413.mrc": {"size": 678, "modify": "20130101000523"},
                "3820230412.xxx": {"size": None, "modify": "20250101000523"},
            }
        elif test_type == "gobi":
            return [
                "3820230411.ord",
                "3820230411.cnt",
                "3820230412.ord",
                "3820230412.cnt",
                "3820230413.cnt",
            ]

    def mock_retrieve_file(*args):
        if args[0] == "1220240402_f.mrc":
            raise ftplib.error_perm("550 The system cannot find the file specified.")

    mock_hook = mocker.MagicMock()
    mock_hook.get_mod_time = mock_get_mod_time
    mock_hook.get_size = mock_get_size
    mock_hook.list_directory = mock_list_directory
    mock_hook.describe_directory = mock_list_directory
    mock_hook.retrieve_file = mock_retrieve_file
    return mock_hook


@pytest.mark.parametrize("mock_hook", ["ftp_download"], indirect=True)
def test_filter_by_strategy_regex(mock_hook, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )

    file_list_by_strategy = filter_by_strategy.function(
        "ftp-example.com-user", "oclc", r"^\d+\.mrc$"
    )
    assert len(file_list_by_strategy) == 3
    assert "Filtered filenames by regex strategy" in caplog.text


@pytest.mark.parametrize("mock_hook", ["ftp_download"], indirect=True)
def test_filter_by_strategy_none(mock_hook, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )
    file_list_by_strategy = filter_by_strategy.function(
        "ftp-example.com-user", "oclc", ""
    )
    assert len(file_list_by_strategy) == 4
    assert "Filenames not filtered" in caplog.text


@pytest.mark.parametrize("mock_hook", ["gobi"], indirect=True)
def test_filter_by_strategy_gobi(mock_hook, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )
    files = filter_by_strategy.function(
        "ftp-example.com-user",
        "orders",
        "CNT-ORD",
    )
    assert len(files) == 2
    assert "Filtered filenames by gobi order strategy" in caplog.text


def test_filter_already_downloaded(pg_hook, mocker, caplog):
    file_list_by_strategy = ["3820230411.mrc", "3820230412.mrc", "3820230413.mrc"]
    files_not_yet_downloaded = filter_already_downloaded.function(
        "oclc",
        "43459f05-f98b-43c0-a79d-76a8855dba94",
        "65d30c15-a560-4064-be92-f90e38eeb351",
        file_list_by_strategy,
    )
    assert len(files_not_yet_downloaded) == 2
    assert "Already downloaded 1 files" in caplog.text


@pytest.mark.parametrize("mock_hook", ["ftp_download"], indirect=True)
def test_filter_by_mod_date(mock_hook, pg_hook, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.Variable.get", return_value="10"
    )
    files_not_yet_downloaded = ["3820230411.mrc", "3820230413.mrc"]
    mod_date_after = datetime.now(timezone.utc) - timedelta(days=int(10))
    filtered_by_timestamp = filter_by_mod_date.function(
        "ftp-example.com-user",
        "oclc",
        "43459f05-f98b-43c0-a79d-76a8855dba94",
        "65d30c15-a560-4064-be92-f90e38eeb351",
        files_not_yet_downloaded,
    )
    assert (
        f"Filtering files modified after {mod_date_after.isoformat(timespec='seconds')}"
        in caplog.text
    )
    assert f"Filtered by mod filenames: {filtered_by_timestamp}" in caplog.text
    assert len(filtered_by_timestamp) == 1
    assert (
        "Adding to VendorFile status: skipped, filename: 3820230413.mrc, file size: 678, vendor uuid: 43459f05-f98b-43c0-a79d-76a8855dba94"
        in caplog.text
    )
    with Session(pg_hook()) as session:
        skipped_vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "3820230413.mrc")
        ).first()
        assert skipped_vendor_file.filesize == 678
        assert skipped_vendor_file.vendor_timestamp == datetime.fromisoformat(
            "2013-01-01T00:05:23"
        )
        assert skipped_vendor_file.status == FileStatus.skipped


@pytest.mark.parametrize("mock_hook", ["ftp_download"], indirect=True)
def test_download_task(mock_hook, download_path, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )
    file_statuses = download_task.function(
        "ftp-example.com-user",
        "oclc",
        download_path,
        "Gobi - Full bibs",
        ["3820230411.mrc"],
    )
    mod_time = (datetime.now(timezone.utc) - timedelta(days=int(5))).isoformat(
        timespec="seconds"
    )
    assert (
        "Downloading for interface Gobi - Full bibs from oclc with ftp-example.com-user"
        in caplog.text
    )
    assert f"Downloading 3820230411.mrc ({mod_time}) to {download_path}/3820230411.mrc"
    assert file_statuses["fetched"] == [("3820230411.mrc", 123, mod_time)]


def test_update_vendor_files_table(pg_hook):
    mod_time = (
        (datetime.now(timezone.utc) - timedelta(days=int(5)))
        .replace(tzinfo=None)
        .isoformat(timespec="seconds")
    )
    file_statuses = {
        "fetched": [
            ("3820230411.mrc", 123, mod_time),
            ("filenameB", 1.3, "2025-11-18T13:55:22"),
        ],
        "fetching_error": [("blah", 0, "2023-01-01T00:05:23")],
        "empty_file_error": [("empty_file.mrc", 0, "2025-01-01T00:05:23")],
    }
    update_vendor_files_table.function(
        file_statuses,
        "43459f05-f98b-43c0-a79d-76a8855dba94",
        "65d30c15-a560-4064-be92-f90e38eeb351",
    )
    with Session(pg_hook()) as session:
        vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "3820230411.mrc")
        ).first()
        assert vendor_file.vendor_interface_id == 1
        assert vendor_file.filesize == 123
        assert vendor_file.status == FileStatus.fetched
        assert vendor_file.vendor_timestamp == datetime.fromisoformat(mod_time)
        second_vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "filenameB")
        ).first()
        assert second_vendor_file.status == FileStatus.fetched
        assert second_vendor_file.filesize == 1.3
        errored_vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "blah")
        ).first()
        assert errored_vendor_file.status == FileStatus.fetching_error
        assert errored_vendor_file.filesize == 0
        empty_vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "empty_file.mrc")
        ).first()
        assert empty_vendor_file.status == FileStatus.empty_file_error
        assert empty_vendor_file.filesize == 0


@pytest.mark.parametrize("mock_hook", ["ftp_error"], indirect=True)
def test_ftp_download_error_handling(mock_hook, mocker, caplog):

    adapter = FTPAdapter(hook=mock_hook, remote_path="oclc")

    mod_time = adapter.get_mod_time('0820240402_f.mrc')

    assert "Failed to retrieve modified time" in caplog.text
    assert mod_time == "2024-04-11T19:31:59"

    file_size = adapter.get_size('1120240402_f.mrc')

    assert "Failed to retrieve size" in caplog.text
    assert file_size == 563

    adapter.retrieve_file("1220240402_f.mrc", "/home/airflow/vendor-files/abded-abcd")

    assert "Failed to retrieve 1220240402_f.mrc" in caplog.text


@pytest.mark.parametrize("mock_hook", ["sftp_download"], indirect=True)
def test_sftp_adapter(mock_hook):
    adapter = SFTPAdapter(hook=mock_hook, remote_path="oclc")
    list_dir = adapter.list_directory()
    assert len(list_dir) == 4
    mod_time = adapter.get_mod_time("3820230411.mrc")
    assert mod_time == "2023-01-01T00:05:23"
    file_size = adapter.get_size("3820230411.mrc")
    assert file_size == 123
    no_file_size = adapter.get_size("3820230412.xxx")
    assert no_file_size == 0


def test_filter_remote_path():
    filename = "Stanford/ST26673.mrc"
    filtered_filename = _filter_remote_path(filename, "Stanford")
    assert filtered_filename == "ST26673.mrc"
