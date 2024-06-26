import pytest  # noqa
import pathlib
import httpx

from http import HTTPStatus

from airflow.models import Connection

import libsys_airflow.plugins.data_exports.transmission_tasks as transmission_tasks

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    gather_files_task,
    retry_failed_files_task,
    transmit_data_http_task,
    transmit_data_ftp_task,
    oclc_connections,
    archive_transmitted_data_task,
    gather_oclc_files_task,
)


@pytest.fixture(params=["pod", "gobi"])
def mock_vendor_marc_files(tmp_path, request):
    airflow = tmp_path / "airflow"
    vendor = request.param
    vendor_dir = airflow / f"data-export-files/{vendor}/"
    marc_file_dir = vendor_dir / "marc-files" / "updates"
    marc_file_dir.mkdir(parents=True)
    setup_files = {
        "filenames": [
            "2024022914.mrc",
            "2024030114.mrc",
            "2024030214.mrc",
            "2024030214.txt",
        ]
    }
    files = []
    for i, x in enumerate(setup_files['filenames']):
        file = pathlib.Path(f"{marc_file_dir}/{x}")
        file.touch()
        if i in [0, 3, 4]:
            file.write_text("hello world")
        files.append(str(file))
    return {"file_list": files, "s3": False}


@pytest.fixture
def mock_file_system(tmp_path):
    airflow = tmp_path / "airflow"
    vendor_dir = airflow / "data-export-files/some-vendor/"
    marc_file_dir = vendor_dir / "marc-files" / "updates"
    marc_file_dir.mkdir(parents=True)
    instance_id_dir = vendor_dir / "instanceids" / "updates"
    instance_id_dir.mkdir(parents=True)
    archive_dir = vendor_dir / "transmitted" / "updates"
    archive_dir.mkdir(parents=True)

    return [airflow, marc_file_dir, instance_id_dir, archive_dir]


@pytest.fixture
def mock_marc_files(mock_file_system):
    marc_file_dir = mock_file_system[1]
    setup_marc_files = {
        "marc_files": [
            "2024022914.mrc",
            "2024030114.mrc",
            "2024030214.mrc",
        ]
    }
    marc_files = []
    for i, x in enumerate(setup_marc_files['marc_files']):
        marc_file = pathlib.Path(f"{marc_file_dir}/{x}")
        marc_file.touch()
        if i == 0:
            marc_file.write_text("hello world")
        marc_files.append(str(marc_file))

    return {"file_list": marc_files, "s3": False}


@pytest.fixture
def mock_httpx_connection():
    return Connection(
        conn_id="http-example.com",
        conn_type="http",
        host="https://www.example.com",
        login=None,
        password=None,
        extra={"Authorization": "access_token"},
        schema="https",
    )


@pytest.fixture
def mock_ftphook_connection():
    return Connection(
        conn_id="ftp-example.com",
        conn_type="ftp",
        host="ftp://www.example.com",
        login="username",
        password="pass",
        extra={"remote_path": "/remote/path/dir"},
        schema="ftp",
    )


@pytest.fixture
def mock_oclc_connection():
    return Connection(
        conn_id="http.oclc-LIB",
        conn_type="http",
        host=None,
        login="client_id",
        password="secret",
        extra={"oclc_code": "LIB"},
    )


@pytest.fixture
def mock_httpx_success():
    return httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(HTTPStatus.OK))
    )


@pytest.fixture
def mock_httpx_failure():
    return httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )


@pytest.mark.parametrize("mock_vendor_marc_files", ["pod"], indirect=True)
def test_gather_files_task(tmp_path, mock_vendor_marc_files):
    airflow = tmp_path / "airflow"
    marc_files = gather_files_task.function(airflow=airflow, vendor="pod")
    assert marc_files["file_list"][0] == mock_vendor_marc_files["file_list"][0]


def test_gather_full_dump_files(mocker):
    mocker.patch.object(transmission_tasks, "S3Path", pathlib.Path)
    marc_files = gather_files_task.function(
        vendor="full-dump", params={"bucket": "data-export-test"}
    )
    assert marc_files["s3"]


@pytest.mark.parametrize("mock_vendor_marc_files", ["gobi"], indirect=True)
def test_gather_gobi_files(tmp_path, mock_vendor_marc_files):
    airflow = tmp_path / "airflow"
    marc_files = gather_files_task.function(airflow=airflow, vendor="gobi")
    assert marc_files["file_list"][0] == mock_vendor_marc_files["file_list"][-1]
    assert len(marc_files["file_list"]) == 1


def test_retry_failed_files_task(mock_marc_files, caplog):
    retry_failed_files_task.function(files=mock_marc_files["file_list"])
    assert "Retry failed files" in caplog.text
    retry_failed_files_task.function(files=[])
    assert "No failures to retry" in caplog.text


def test_transmit_data_ftp_task(
    mocker, mock_ftphook_connection, mock_marc_files, caplog
):
    ftp_hook = mocker.patch(
        "airflow.providers.ftp.hooks.ftp.FTPHook.store_file", return_value=True
    )
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.Connection.get_connection_from_secrets",
        return_value=mock_ftphook_connection,
    )

    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.is_production",
        return_value=True,
    )

    transmit_data = transmit_data_ftp_task.function("ftp-example.com", mock_marc_files)
    assert len(transmit_data["success"]) == 3
    assert "Start transmission of file" in caplog.text
    assert ftp_hook.store_file.called_with(
        "/remote/path/dir/2024022914.mrc", "2024022914.mrc"
    )


@pytest.mark.parametrize("mock_vendor_marc_files", ["gobi"], indirect=True)
def test_transmit_gobi_data_ftp_task(
    mocker, mock_ftphook_connection, mock_vendor_marc_files, tmp_path, caplog
):
    ftp_hook = mocker.patch(
        "airflow.providers.ftp.hooks.ftp.FTPHook.store_file", return_value=True
    )
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.Connection.get_connection_from_secrets",
        return_value=mock_ftphook_connection,
    )

    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.is_production",
        return_value=True,
    )

    airflow = tmp_path / "airflow"
    marc_files = gather_files_task.function(airflow=airflow, vendor="gobi")
    transmit_data = transmit_data_ftp_task.function("ftp-example.com", marc_files)
    assert len(transmit_data["success"]) == 1
    assert "Start transmission of file" in caplog.text
    assert ftp_hook.store_file.called_with(
        "/remote/path/dir/2024030214.txt", "stf.2024030214.txt"
    )


def test_transmit_data_task(
    mocker, mock_httpx_connection, mock_httpx_success, mock_marc_files, caplog
):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.httpx.Client",
        return_value=mock_httpx_success,
    )
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.Connection.get_connection_from_secrets",
        return_value=mock_httpx_connection,
    )
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.is_production",
        return_value=True,
    )
    transmit_data = transmit_data_http_task.function(
        mock_marc_files, files_params="upload[files][]", params={"vendor": "pod"}
    )
    assert len(transmit_data["success"]) == 3
    assert "Transmit data to pod" in caplog.text


def test_transmit_data_from_s3_task(
    mocker, mock_httpx_connection, mock_httpx_success, mock_marc_files, caplog
):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.httpx.Client",
        return_value=mock_httpx_success,
    )
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.Connection.get_connection_from_secrets",
        return_value=mock_httpx_connection,
    )
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.is_production",
        return_value=True,
    )
    mocker.patch.object(transmission_tasks, "S3Path", pathlib.Path)
    mock_marc_files["s3"] = True

    transmit_data_from_s3 = transmit_data_http_task.function(
        mock_marc_files,
        files_params="upload[files][]",
        params={"vendor": "pod", "bucket": "data-export-test"},
    )
    assert len(transmit_data_from_s3["success"]) == 3


def test_transmit_data_failed(
    mocker, mock_httpx_connection, mock_httpx_failure, mock_marc_files, caplog
):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.httpx.Client",
        return_value=mock_httpx_failure,
    )
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.Connection.get_connection_from_secrets",
        return_value=mock_httpx_connection,
    )
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.is_production",
        return_value=True,
    )
    transmit_data = transmit_data_http_task.function(
        mock_marc_files,
        params={"vendor": "pod"},
    )
    assert len(transmit_data["failures"]) == 3
    assert "Transmit data to pod" in caplog.text


def test_oclc_connections(mocker, mock_oclc_connection):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.Connection.get_connection_from_secrets",
        return_value=mock_oclc_connection,
    )
    connection_lookup = oclc_connections(["http.oclc-LIB"])
    assert connection_lookup["LIB"]["username"] == "client_id"
    assert connection_lookup["LIB"]["password"] == "secret"


def test_archive_transmitted_data_task(mock_file_system, mock_marc_files):
    instance_id_dir = mock_file_system[2]
    instance_id_file1 = instance_id_dir / "2024022914.csv"
    instance_id_file1.touch()
    mock_marc_file_list = mock_marc_files["file_list"]
    archive_transmitted_data_task.function(mock_marc_file_list)
    assert not instance_id_file1.exists()
    transmitted_dir = mock_file_system[-1]
    for x in mock_marc_file_list:
        assert (transmitted_dir / pathlib.Path(x).name).exists()

    assert (transmitted_dir / instance_id_file1.name).exists()


@pytest.mark.parametrize("mock_vendor_marc_files", ["gobi"], indirect=True)
def test_archive_gobi_files(tmp_path, mock_vendor_marc_files):
    airflow = tmp_path / "airflow"
    vendor_dir = airflow / "data-export-files/gobi/"
    instance_id_dir = vendor_dir / "instanceids" / "updates"
    instance_id_dir.mkdir(parents=True)
    instance_id_file1 = instance_id_dir / "2024030214.csv"
    instance_id_file1.touch()
    archive_dir = vendor_dir / "transmitted" / "updates"
    archive_dir.mkdir(parents=True)
    transmitted_files = gather_files_task.function(airflow=airflow, vendor="gobi")
    assert len(transmitted_files["file_list"]) == 1
    archive_transmitted_data_task.function(transmitted_files["file_list"])
    related_marc_file = pathlib.Path(transmitted_files["file_list"][0]).stem
    related_marc_file = related_marc_file + ".mrc"
    assert (archive_dir / pathlib.Path(transmitted_files["file_list"][0]).name).exists()
    assert (archive_dir / pathlib.Path(related_marc_file)).exists()
    assert (archive_dir / instance_id_file1.name).exists()


def test_archive_transmitted_data_task_no_files(caplog):
    archive_transmitted_data_task.function([])
    assert "No files to archive" in caplog.text


def test_gather_oclc_files_task(tmp_path):
    airflow = tmp_path
    oclc_marc_path = airflow / "data-export-files/oclc/marc-files"
    oclc_marc_path.mkdir(parents=True, exist_ok=True)

    new_path = oclc_marc_path / "new"
    new_path.mkdir(parents=True, exist_ok=True)
    new_stf_marc_file = new_path / "20240603113-STF.mrc"
    new_stf_marc_file.touch()
    new_casum = new_path / "20240603113-CASUM.mrc"
    new_casum.touch()

    deletes_path = oclc_marc_path / "deletes"
    deletes_path.mkdir(parents=True, exist_ok=True)
    deletes_s7z = deletes_path / "20240603113-S7Z.mrc"
    deletes_s7z.touch()
    deletes_rcj = deletes_path / "2024060313-RCJ.mrc"
    deletes_rcj.touch()

    updates_path = oclc_marc_path / "updates"
    updates_path.mkdir(parents=True, exist_ok=True)
    updates_hin = updates_path / "20240603113-HIN.mrc"
    updates_hin.touch()
    updates_stf = updates_path / "20240603113-STF.mrc"
    updates_stf.touch()

    libraries = gather_oclc_files_task.function(airflow=airflow)

    assert libraries["STF"] == {
        "new": [str(new_stf_marc_file)],
        "updates": [str(updates_stf)],
    }

    assert len(libraries["HIN"]["updates"]) == 1
    assert "deletes" in libraries["RCJ"]
    assert "updates" not in libraries["CASUM"]
