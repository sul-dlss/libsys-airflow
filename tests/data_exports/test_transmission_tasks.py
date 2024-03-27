import pytest  # noqa
import pathlib
import httpx

from http import HTTPStatus

from airflow.models import Connection

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    gather_files_task,
    transmit_data_http_task,
    archive_transmitted_data_task,
)


@pytest.fixture
def mock_file_system(tmp_path):
    airflow = tmp_path / "airflow"
    vendor_dir = airflow / "data-export-files/oclc/"
    marc_file_dir = vendor_dir / "marc-files"
    marc_file_dir.mkdir(parents=True)
    instance_id_dir = vendor_dir / "instanceids"
    instance_id_dir.mkdir()
    archive_dir = vendor_dir / "transmitted"
    archive_dir.mkdir()

    return [airflow, marc_file_dir, instance_id_dir, archive_dir]


# Mock xcom messages dict
@pytest.fixture
def mock_marc_file_list(mock_file_system):
    marc_file_dir = mock_file_system[1]
    setup_marc_files = {
        "marc_file_list": ["2024022914.mrc", "2024030114.mrc", "2024030214.mrc"]
    }
    marc_file_list = []
    for x in setup_marc_files['marc_file_list']:
        marc_file = pathlib.Path(f"{marc_file_dir}/{x}")
        marc_file.touch()
        marc_file_list.append(str(marc_file))

    return marc_file_list


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
def mock_httpx_success():
    return httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(HTTPStatus.OK))
    )


@pytest.fixture
def mock_httpx_failure():
    return httpx.Client(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )


def test_gather_files_task(mock_file_system, mock_marc_file_list):
    airflow = mock_file_system[0]
    marc_file_list = gather_files_task.function(airflow=airflow, vendor="oclc")
    assert marc_file_list.sort() == mock_marc_file_list.sort()


def test_transmit_data_task(
    mocker, mock_httpx_connection, mock_httpx_success, mock_marc_file_list
):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.httpx.Client",
        return_value=mock_httpx_success,
    )
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.Connection.get_connection_from_secrets",
        return_value=mock_httpx_connection,
    )
    transmit_data = transmit_data_http_task.function(
        "vendor",
        mock_marc_file_list,
        files_params="upload[files][]",
    )
    for x in mock_marc_file_list:
        assert len(transmit_data["success"]) == 3


def test_transmit_data_failed(
    mocker, mock_httpx_connection, mock_httpx_failure, mock_marc_file_list
):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.httpx.Client",
        return_value=mock_httpx_failure,
    )
    mocker.patch(
        "libsys_airflow.plugins.data_exports.transmission_tasks.Connection.get_connection_from_secrets",
        return_value=mock_httpx_connection,
    )
    transmit_data = transmit_data_http_task.function(
        "vendor",
        mock_marc_file_list,
    )
    for x in mock_marc_file_list:
        assert len(transmit_data["failures"]) == 3


def test_archive_transmitted_data_task(mock_file_system, mock_marc_file_list):
    instance_id_dir = mock_file_system[2]
    instance_id_file1 = instance_id_dir / "2024022914.csv"
    instance_id_file1.touch()
    archive_transmitted_data_task.function(mock_marc_file_list)
    assert not instance_id_file1.exists()
    transmitted_dir = mock_file_system[-1]
    for x in mock_marc_file_list:
        assert (transmitted_dir / pathlib.Path(x).name).exists()

    assert (transmitted_dir / instance_id_file1.name).exists()


def test_archive_transmitted_data_task_no_files(caplog):
    archive_transmitted_data_task.function([])
    assert "No files to archive" in caplog.text
