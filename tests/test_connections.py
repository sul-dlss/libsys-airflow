import pytest

from unittest.mock import MagicMock
from airflow_client.client.rest import ApiException

from mocks import (  # noqa
    MockAirflowApiClientConfig,
    MockAirflowApiClient,
)

from libsys_airflow.plugins.airflow.connections import (
    find_or_create_conn,
    create_connection,
)


def mock_api_instance():
    api_instance = MagicMock()
    api_instance.get_connection.return_value = MagicMock(
        connection_id="ftp-example.com-user"
    )
    return api_instance


def mock_api_instance_not_found():
    api_instance = MagicMock()

    def mock_get_connection(*args, **kwargs):
        raise ApiException("Connection not found")

    api_instance.get_connection = mock_get_connection
    api_instance.post_connection.return_value = MagicMock(
        connection_id="ftp-example.com-user"
    )
    return api_instance


@pytest.fixture
def mock_client_config():
    return MockAirflowApiClientConfig()


@pytest.fixture
def mock_api_client():
    return MockAirflowApiClient(configuration=MockAirflowApiClientConfig())


def test_connection_already_exists(mock_api_client, mocker):
    mocker.patch(
        "libsys_airflow.plugins.airflow.connections.api_client",
        return_value=mock_api_client,
    )
    mocker.patch(
        "libsys_airflow.plugins.airflow.connections.airflow_client.client.ConnectionApi",
        return_value=mock_api_instance(),
    )

    conn_id = find_or_create_conn("ftp", "example.com", "user", "pass")
    assert conn_id == "ftp-example.com-user"


def test_create_connection_ftp(mock_api_client, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.airflow.connections.api_client",
        return_value=mock_api_client,
    )
    mocker.patch(
        "libsys_airflow.plugins.airflow.connections.airflow_client.client.ConnectionApi",
        return_value=mock_api_instance_not_found(),
    )
    conn_id = find_or_create_conn("ftp", "example.com", "user", "pass")
    assert conn_id == "ftp-example.com-user"
    assert "Exception when calling ConnectionApi" in caplog.text


def test_create_connection_sftp_with_keyfile(mocker):
    mock_find_or_create_conn = mocker.patch(
        "libsys_airflow.plugins.airflow.connections.find_or_create_conn",
        return_value="sftp-sftp.amalivre.fr-user",
    )
    mocker.patch(
        "libsys_airflow.plugins.airflow.connections.interface_info",
        return_value={
            "uri": "sftp://sftp.amalivre.fr",
            "username": "user",
            "password": None,  # noqa: S105
        },
    )
    conn_id = create_connection("1234")
    assert conn_id == "sftp-sftp.amalivre.fr-user"
    mock_find_or_create_conn.assert_called_once_with(
        "sftp",
        "sftp.amalivre.fr",
        "user",
        None,
        None,
        "/opt/airflow/vendor-keys/sftp.amalivre.fr",
    )


def test_create_connection_sftp(mocker):
    mocker.patch(
        "libsys_airflow.plugins.airflow.connections.interface_info",
        return_value={
            "uri": "xftp://example.com",
            "username": "user",
            "password": "pass",  # noqa: S105
        },
    )
    with pytest.raises(ValueError):
        create_connection("1234")
