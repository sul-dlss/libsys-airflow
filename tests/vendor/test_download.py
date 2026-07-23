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


@pytest.fixture(params=["ftp_download", "sftp_download", "gobi"])
def mock_hook(mocker, request):
    test_type = request.param

    def mock_describe_directory(*args):
        if test_type == "ftp_download":
            five_days_ago = (
                datetime.now(timezone.utc) - timedelta(days=int(5))
            ).replace(microsecond=0)
            return {
                "3820230411.mrc": {
                    "size": "123",
                    "modify": five_days_ago.strftime("%Y%m%d%H%M%S"),
                    "type": "file",
                },
                "3820230412.mrc": {
                    "size": "456",
                    "modify": "20230101000523",
                    "type": "file",
                },
                "3820230413.mrc": {
                    "size": "678",
                    "modify": "20130101000523",
                    "type": "file",
                },
                "3820230412.xxx": {
                    "size": "999",
                    "modify": "20250101000523",
                    "type": "file",
                },
            }
        elif test_type == "sftp_download":
            return {
                "3820230411.mrc": {
                    "size": "123",
                    "modify": "20230101000523",
                    "type": "file",
                },
                "3820230412.mrc": {
                    "size": "456",
                    "modify": "20230101000523",
                    "type": "file",
                },
                "3820230413.mrc": {
                    "size": "678",
                    "modify": "20130101000523",
                    "type": "file",
                },
                "3820230412.xxx": {
                    "size": None,
                    "modify": "20250101000523",
                    "type": "file",
                },
            }
        elif test_type == "gobi":
            return {
                "3820230411.ord": {
                    "size": "100",
                    "modify": "20230411120000",
                    "type": "file",
                },
                "3820230411.cnt": {
                    "size": "200",
                    "modify": "20230411120000",
                    "type": "file",
                },
                "3820230412.ord": {
                    "size": "300",
                    "modify": "20230412120000",
                    "type": "file",
                },
                "3820230412.cnt": {
                    "size": "400",
                    "modify": "20230412120000",
                    "type": "file",
                },
                "3820230413.cnt": {
                    "size": "500",
                    "modify": "20230413120000",
                    "type": "file",
                },
            }

    def mock_retrieve_file(*args):
        if args[0] == "1220240402_f.mrc":
            raise ftplib.error_perm("550 The system cannot find the file specified.")

    mock_hook = mocker.MagicMock()
    mock_hook.describe_directory = mock_describe_directory
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
    assert len(file_list_by_strategy["filtered_files"]) == 3
    assert "Filtered filenames by regex strategy" in caplog.text


@pytest.mark.parametrize("mock_hook", ["ftp_download"], indirect=True)
def test_filter_by_strategy_none(mock_hook, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )
    file_list_by_strategy = filter_by_strategy.function(
        "ftp-example.com-user", "oclc", ""
    )
    assert len(file_list_by_strategy["all_files"]) == 4
    assert len(file_list_by_strategy["filtered_files"]) == 4
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
    assert len(files["all_files"]) == 5
    assert len(files["filtered_files"]) == 2
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
        files_not_yet_downloaded,
    )
    assert (
        f"Filtering files modified after {mod_date_after.isoformat(timespec='seconds')}"
        in caplog.text
    )
    assert len(filtered_by_timestamp["filtered_files"]) == 1
    assert len(filtered_by_timestamp["skipped"]) == 1
    assert filtered_by_timestamp["skipped"].pop() == (
        "3820230413.mrc",
        678,
        "2013-01-01T00:05:23",
    )


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
    mod_time = (
        (datetime.now(timezone.utc) - timedelta(days=int(5)))
        .replace(tzinfo=None)
        .isoformat(timespec="seconds")
    )
    assert (
        "Downloading for interface Gobi - Full bibs from oclc with ftp-example.com-user"
        in caplog.text
    )
    assert f"Downloading 3820230411.mrc ({mod_time}) to {download_path}/3820230411.mrc"
    assert file_statuses["fetched"] == [("3820230411.mrc", 123, mod_time)]


def test_update_vendor_files_table(pg_hook, caplog):
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
        "skipped": [("3820230413.mrc", 678, "2013-01-01T00:05:23")],
    }
    update_vendor_files_table.function(
        file_statuses,
        "43459f05-f98b-43c0-a79d-76a8855dba94",
        "65d30c15-a560-4064-be92-f90e38eeb351",
    )
    assert (
        "Adding to VendorFile status: skipped, filename: 3820230413.mrc, file size: 678, vendor uuid: 43459f05-f98b-43c0-a79d-76a8855dba94"
        in caplog.text
    )
    assert (
        "Adding to VendorFile status: fetched, filename: 3820230411.mrc, file size: 123, vendor uuid: 43459f05-f98b-43c0-a79d-76a8855dba94"
        in caplog.text
    )
    assert (
        "Adding to VendorFile status: fetched, filename: filenameB, file size: 1.3, vendor uuid: 43459f05-f98b-43c0-a79d-76a8855dba94"
        in caplog.text
    )
    assert (
        "Adding to VendorFile status: fetching_error, filename: blah, file size: 0, vendor uuid: 43459f05-f98b-43c0-a79d-76a8855dba94"
        in caplog.text
    )
    assert (
        "Adding to VendorFile status: empty_file_error, filename: empty_file.mrc, file size: 0, vendor uuid: 43459f05-f98b-43c0-a79d-76a8855dba94"
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


def test_ftp_adapter_fallback_to_list_directory(mocker):
    mock_hook = mocker.MagicMock()
    mock_hook.describe_directory.side_effect = ftplib.error_perm(
        "502 MLSD not implemented"
    )
    mock_hook.list_directory.return_value = ["file1.mrc", "file2.mrc"]
    mock_hook.conn.sendcmd.return_value = None

    # Mock get_mod_time to return datetime objects
    def mock_get_mod_time(path):
        return datetime(2024, 1, 15, 10, 30, 0)

    mock_hook.get_mod_time.side_effect = mock_get_mod_time

    # Mock get_size to return integers
    mock_hook.get_size.return_value = 1234

    adapter = FTPAdapter(mock_hook, "/remote/path")

    assert mock_hook.list_directory.called
    assert adapter.list_directory() == ["file1.mrc", "file2.mrc"]
    assert adapter.get_size("file1.mrc") == 1234
    assert adapter.get_mod_time("file1.mrc") == "2024-01-15T10:30:00"


def test_build_descriptions_handles_errors(mocker):
    mock_hook = mocker.MagicMock()
    mock_hook.describe_directory.side_effect = ftplib.error_perm(
        "502 MLSD not implemented"
    )
    mock_hook.list_directory.return_value = ["good_file.mrc", "bad_file.mrc"]
    mock_hook.conn.sendcmd.return_value = None

    # First file succeeds, second fails
    def mock_get_mod_time(path):
        if "bad_file" in path:
            raise ftplib.error_perm("550 File not found")
        return datetime(2024, 1, 15, 10, 30, 0)

    def mock_get_size(path):
        if "bad_file" in path:
            raise ftplib.error_perm("550 File not found")
        return 1234

    mock_hook.get_mod_time.side_effect = mock_get_mod_time
    mock_hook.get_size.side_effect = mock_get_size

    adapter = FTPAdapter(mock_hook, "/remote/path")

    # Good file has real data
    assert adapter.get_size("good_file.mrc") == 1234
    assert adapter.get_mod_time("good_file.mrc") == "2024-01-15T10:30:00"

    # Bad file has defaults
    assert adapter.get_size("bad_file.mrc") == 0
    assert adapter.get_mod_time("bad_file.mrc") == "1970-01-01T00:00:00"


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
