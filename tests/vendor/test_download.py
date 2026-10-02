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
    _download_cutoff,
    _filter_already_downloaded,
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
    VendorFile(
        created=datetime.now(timezone.utc),
        updated=datetime.now(timezone.utc),
        vendor_interface_id=1,
        vendor_filename="recent_skipped.mrc",
        filesize=0,
        status=FileStatus.skipped,
        vendor_timestamp=datetime.now(timezone.utc).replace(tzinfo=None)
        - timedelta(days=5),
    ),
    VendorFile(
        created=datetime.now(timezone.utc),
        updated=datetime.now(timezone.utc),
        vendor_interface_id=1,
        vendor_filename="old_skipped.mrc",
        filesize=0,
        status=FileStatus.skipped,
        vendor_timestamp=datetime.fromisoformat("1970-01-01T00:00:00"),
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

    def mock_list_directory(*args):
        return list(mock_describe_directory())

    def mock_get_mod_time(path):
        facts = mock_describe_directory().get(path.split("/")[-1])
        if facts is None:
            raise ftplib.error_perm("550 No such file or directory")
        return datetime.strptime(facts["modify"], "%Y%m%d%H%M%S")

    def mock_retrieve_file(*args):
        if args[0] == "1220240402_f.mrc":
            raise ftplib.error_perm("550 The system cannot find the file specified.")
        with open(args[1], "wb") as fo:
            fo.write(b"" if args[0].endswith("empty.mrc") else b"x" * 123)

    mock_hook = mocker.MagicMock()
    mock_hook.describe_directory = mock_describe_directory
    mock_hook.list_directory = mock_list_directory
    mock_hook.get_mod_time = mock_get_mod_time
    mock_hook.get_conn.return_value.cwd.side_effect = ftplib.error_perm(
        "550 Not a directory"
    )
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
    assert "Found 4 files in oclc, 3 matched the filter strategy" in caplog.text


@pytest.mark.parametrize("mock_hook", ["ftp_download"], indirect=True)
def test_filter_by_strategy_no_matches(mock_hook, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )

    file_list_by_strategy = filter_by_strategy.function(
        "ftp-example.com-user", "oclc", r"^\d+\.pdf$"
    )
    assert file_list_by_strategy == {"filtered_files": []}
    assert "Found 4 files in oclc, 0 matched the filter strategy" in caplog.text
    assert "No files matched; first 20 files: ['3820230411.mrc'" in caplog.text


@pytest.mark.parametrize("mock_hook", ["ftp_download"], indirect=True)
def test_filter_by_strategy_none(mock_hook, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )
    file_list_by_strategy = filter_by_strategy.function(
        "ftp-example.com-user", "oclc", ""
    )
    assert len(file_list_by_strategy["filtered_files"]) == 4
    assert "Filenames not filtered" in caplog.text
    assert "Found 4 files in oclc, 4 matched the filter strategy" in caplog.text


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
    assert len(files["filtered_files"]) == 2
    assert "Found 5 files in orders, 2 matched the filter strategy" in caplog.text
    assert "Filtered filenames by gobi order strategy" in caplog.text


def test_filter_already_downloaded(pg_hook, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.Variable.get", return_value="10"
    )
    file_list_by_strategy = ["3820230411.mrc", "3820230412.mrc", "3820230413.mrc"]
    files_not_yet_downloaded = filter_already_downloaded.function(
        "oclc",
        "43459f05-f98b-43c0-a79d-76a8855dba94",
        "65d30c15-a560-4064-be92-f90e38eeb351",
        file_list_by_strategy,
    )
    assert len(files_not_yet_downloaded) == 2
    assert "Already downloaded 1 files" in caplog.text


@pytest.mark.parametrize(
    "download_days_ago,not_yet_downloaded",
    [("10", ["recent_skipped.mrc"]), ("3", [])],
)
def test_filter_already_downloaded_skipped_files(
    pg_hook, engine, mocker, download_days_ago, not_yet_downloaded
):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.Variable.get",
        return_value=download_days_ago,
    )
    files = _filter_already_downloaded(
        ["recent_skipped.mrc", "old_skipped.mrc"],
        "oclc",
        "43459f05-f98b-43c0-a79d-76a8855dba94",
        "65d30c15-a560-4064-be92-f90e38eeb351",
        engine,
    )
    assert files["not_yet_downloaded"] == not_yet_downloaded
    assert "old_skipped.mrc" in files["already_downloaded"]


@pytest.mark.parametrize("download_days_ago,days", [(10, 10), ("30", 30), ("0", 0)])
def test_download_cutoff(mocker, download_days_ago, days):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.Variable.get",
        return_value=download_days_ago,
    )
    expected = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)
    assert abs(_download_cutoff() - expected) < timedelta(minutes=1)


@pytest.mark.parametrize("mock_hook", ["ftp_download"], indirect=True)
def test_filter_by_mod_date(mock_hook, pg_hook, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.Variable.get", return_value="10"
    )
    files_not_yet_downloaded = ["3820230411.mrc", "3820230413.mrc"]
    filtered_by_timestamp = filter_by_mod_date.function(
        "ftp-example.com-user",
        "oclc",
        files_not_yet_downloaded,
    )
    assert "Filtering files modified after" in caplog.text
    [(filename, mod_time)] = filtered_by_timestamp["filtered_files"]
    assert filename == "3820230411.mrc"
    five_days_ago = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=5)
    assert abs(datetime.fromisoformat(mod_time) - five_days_ago) < timedelta(minutes=1)
    assert filtered_by_timestamp["skipped"] == [
        ("3820230413.mrc", 0, "2013-01-01T00:05:23")
    ]
    assert filtered_by_timestamp["fetching_error"] == []


@pytest.mark.parametrize("mock_hook", ["ftp_download"], indirect=True)
def test_filter_by_mod_date_file_not_in_listing(mock_hook, mocker):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.Variable.get", return_value="10"
    )
    filtered_by_timestamp = filter_by_mod_date.function(
        "ftp-example.com-user", "oclc", ["3820230413.mrc", "gone.mrc"]
    )
    assert filtered_by_timestamp["fetching_error"] == [("gone.mrc", 0, None)]
    assert filtered_by_timestamp["skipped"] == [
        ("3820230413.mrc", 0, "2013-01-01T00:05:23")
    ]


@pytest.mark.parametrize("mock_hook", ["ftp_download"], indirect=True)
def test_download_task(mock_hook, download_path, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )
    mod_time = (
        (datetime.now(timezone.utc) - timedelta(days=int(5)))
        .replace(tzinfo=None)
        .isoformat(timespec="seconds")
    )
    file_statuses = download_task.function(
        "ftp-example.com-user",
        "oclc",
        download_path,
        "Gobi - Full bibs",
        [("3820230411.mrc", mod_time)],
    )
    assert (
        "Downloading for interface Gobi - Full bibs from oclc with ftp-example.com-user"
        in caplog.text
    )
    assert f"Downloading 3820230411.mrc ({mod_time}) to {download_path}/3820230411.mrc"
    assert file_statuses["fetched"] == [("3820230411.mrc", 123, mod_time)]


@pytest.mark.parametrize("mock_hook", ["ftp_download"], indirect=True)
def test_download_task_empty_file(mock_hook, download_path, mocker):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook", return_value=mock_hook
    )
    file_statuses = download_task.function(
        "ftp-example.com-user",
        "oclc",
        download_path,
        "Gobi - Full bibs",
        [("empty.mrc", "2025-01-01T00:05:23")],
    )
    assert file_statuses["fetched"] == []
    assert file_statuses["empty_file_error"] == [
        ("empty.mrc", 0, "2025-01-01T00:05:23")
    ]


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
        "fetching_error": [
            ("blah", 0, "2023-01-01T00:05:23"),
            ("no_mod_time", 0, None),
        ],
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
        no_mod_time_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "no_mod_time")
        ).first()
        assert no_mod_time_file.status == FileStatus.fetching_error
        assert no_mod_time_file.vendor_timestamp is None
        empty_vendor_file = session.scalars(
            select(VendorFile).where(VendorFile.vendor_filename == "empty_file.mrc")
        ).first()
        assert empty_vendor_file.status == FileStatus.empty_file_error
        assert empty_vendor_file.filesize == 0


@pytest.fixture
def mock_ftp_hook(mocker):
    mock_hook = mocker.MagicMock()
    mock_hook.list_directory.return_value = [
        "file1.mrc",
        "/remote/path/file2.mrc",
        "bad_file.mrc",
    ]

    def mock_get_mod_time(path):
        if "bad_file" in path or path.endswith("subdir"):
            raise ftplib.error_perm("550 File not found")
        return datetime(2024, 1, 15, 10, 30, 0)

    def mock_cwd(path):
        if path != "/home/user" and not path.endswith("subdir"):
            raise ftplib.error_perm("550 Not a directory")

    mock_hook.get_mod_time.side_effect = mock_get_mod_time
    mock_hook.get_conn.return_value.pwd.return_value = "/home/user"
    mock_hook.get_conn.return_value.cwd.side_effect = mock_cwd
    return mock_hook


def test_ftp_adapter_list_directory(mock_ftp_hook):
    adapter = FTPAdapter(mock_ftp_hook, "/remote/path")

    assert adapter.list_directory() == ["file1.mrc", "file2.mrc", "bad_file.mrc"]
    mock_ftp_hook.get_mod_time.assert_not_called()


def test_ftp_adapter_get_mod_time_queries_only_requested_file(mock_ftp_hook):
    adapter = FTPAdapter(mock_ftp_hook, "/remote/path")

    assert adapter.get_mod_time("file1.mrc") == "2024-01-15T10:30:00"
    mock_ftp_hook.get_mod_time.assert_called_once_with("/remote/path/file1.mrc")
    mock_ftp_hook.list_directory.assert_not_called()


def test_ftp_adapter_get_mod_time_error(mock_ftp_hook):
    adapter = FTPAdapter(mock_ftp_hook, "/remote/path")

    assert adapter.get_mod_time("bad_file.mrc") is None


def test_ftp_adapter_get_mod_time_drops_fractional_seconds(mock_ftp_hook):
    mock_ftp_hook.get_mod_time.side_effect = None
    mock_ftp_hook.get_mod_time.return_value = datetime(2026, 7, 29, 9, 19, 4, 741000)
    adapter = FTPAdapter(mock_ftp_hook, "/remote/path")

    assert adapter.get_mod_time("file1.mrc") == "2026-07-29T09:19:04"


def test_filter_by_mod_date_mod_time_error(mock_ftp_hook, mocker):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook",
        return_value=mock_ftp_hook,
    )
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.Variable.get", return_value="10"
    )
    filtered_by_timestamp = filter_by_mod_date.function(
        "ftp-example.com-user", "/remote/path", ["file1.mrc", "bad_file.mrc"]
    )
    assert filtered_by_timestamp["fetching_error"] == [("bad_file.mrc", 0, None)]
    assert filtered_by_timestamp["skipped"] == [("file1.mrc", 0, "2024-01-15T10:30:00")]


def test_filter_by_mod_date_ignores_directories(mock_ftp_hook, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.create_hook",
        return_value=mock_ftp_hook,
    )
    mocker.patch(
        "libsys_airflow.plugins.vendor.download.Variable.get", return_value="10"
    )
    filtered_by_timestamp = filter_by_mod_date.function(
        "ftp-example.com-user", "/remote/path", ["subdir", "bad_file.mrc"]
    )
    assert filtered_by_timestamp == {
        "filtered_files": [],
        "skipped": [],
        "fetching_error": [("bad_file.mrc", 0, None)],
    }
    assert "Ignoring directory subdir" in caplog.text
    assert mock_ftp_hook.get_conn.return_value.cwd.call_args_list == [
        mocker.call("/remote/path/subdir"),
        mocker.call("/home/user"),
        mocker.call("/remote/path/bad_file.mrc"),
    ]


def test_sftp_adapter_list_directory_excludes_directories(mocker):
    mock_hook = mocker.MagicMock()
    mock_hook.describe_directory.return_value = {
        ".": {"modify": "20240115103000", "type": "cdir"},
        "..": {"modify": "20240115103000", "type": "pdir"},
        "archive": {"modify": "20240115103000", "type": "dir"},
        "file1.mrc": {"size": "123", "modify": "20240115103000", "type": "file"},
        "file2.mrc": {"size": "456", "modify": "20240115103000"},
    }

    adapter = SFTPAdapter(mock_hook, "/remote/path")

    assert adapter.list_directory() == ["file1.mrc", "file2.mrc"]


def test_ftp_adapter_retrieve_file_sets_binary_mode(mocker):
    mock_hook = mocker.MagicMock()

    adapter = FTPAdapter(mock_hook, "/remote/path")
    adapter.retrieve_file("file1.mrc", "/downloads/file1.mrc")

    mock_hook.get_conn.return_value.sendcmd.assert_called_once_with("TYPE I")
    mock_hook.retrieve_file.assert_called_once_with("file1.mrc", "/downloads/file1.mrc")


def test_ftp_adapter_retrieve_file_uses_remote_path_when_bare_name_missing(mocker):
    mock_hook = mocker.MagicMock()
    mock_hook.get_mod_time.side_effect = ftplib.error_perm(
        "550 /file1.mrc: No such file or directory."
    )

    adapter = FTPAdapter(mock_hook, "/remote/path")
    adapter.retrieve_file("file1.mrc", "/downloads/file1.mrc")

    mock_hook.get_mod_time.assert_called_once_with("file1.mrc")
    mock_hook.retrieve_file.assert_called_once_with(
        "/remote/path/file1.mrc", "/downloads/file1.mrc"
    )


@pytest.mark.parametrize("mock_hook", ["sftp_download"], indirect=True)
def test_sftp_adapter(mock_hook):
    adapter = SFTPAdapter(hook=mock_hook, remote_path="oclc")
    list_dir = adapter.list_directory()
    assert len(list_dir) == 4
    mod_time = adapter.get_mod_time("3820230411.mrc")
    assert mod_time == "2023-01-01T00:05:23"
    assert adapter.get_mod_time("gone.mrc") is None


def test_ftp_adapter_does_not_list_on_init(mocker):
    mock_hook = mocker.MagicMock()

    adapter = FTPAdapter(mock_hook, "/remote/path")
    adapter.retrieve_file("file1.mrc", "/downloads/file1.mrc")

    mock_hook.list_directory.assert_not_called()


def test_sftp_adapter_does_not_list_on_init(mocker):
    mock_hook = mocker.MagicMock()
    mock_hook.describe_directory.return_value = {
        "file1.mrc": {"size": 123, "modify": "20240115103000", "type": "file"}
    }

    adapter = SFTPAdapter(mock_hook, "/remote/path")
    adapter.retrieve_file("file1.mrc", "/downloads/file1.mrc")
    mock_hook.describe_directory.assert_not_called()

    assert adapter.list_directory() == ["file1.mrc"]
    assert adapter.get_mod_time("file1.mrc") == "2024-01-15T10:30:00"
    mock_hook.describe_directory.assert_called_once_with("/remote/path")


def test_filter_remote_path():
    filename = "Stanford/ST26673.mrc"
    filtered_filename = _filter_remote_path(filename, "Stanford")
    assert filtered_filename == "ST26673.mrc"
