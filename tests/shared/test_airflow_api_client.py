import httpx
import pytest

from libsys_airflow.plugins.shared import airflow_api_client

KEYCLOAK = (
    "airflow.providers.keycloak.auth_manager.keycloak_auth_manager.KeycloakAuthManager"
)
SIMPLE = (
    "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
)


@pytest.fixture
def keycloak_auth(mocker, monkeypatch):
    monkeypatch.setenv("AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_ID", "airflow-sso")
    monkeypatch.setenv("AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_SECRET", "s3cret")
    mocker.patch.object(airflow_api_client.conf, "get", return_value=KEYCLOAK)


@pytest.fixture
def simple_auth(mocker, monkeypatch):
    monkeypatch.setenv("AIRFLOW_VAR_API_USER", "airflow")
    monkeypatch.setenv("AIRFLOW_VAR_API_PASSWORD", "airflow")
    mocker.patch.object(airflow_api_client.conf, "get", return_value=SIMPLE)


@pytest.fixture
def token_response(mocker):
    return mocker.patch.object(
        airflow_api_client.httpx,
        "post",
        return_value=httpx.Response(201, json={"access_token": "abc123"}),
    )


def test_client_credentials_grant_under_keycloak(keycloak_auth, token_response):
    token = airflow_api_client.get_access_token(host="http://airflow-apiserver:8080")

    assert token == "abc123"
    assert token_response.call_args.args == (
        "http://airflow-apiserver:8080/auth/token",
    )
    assert token_response.call_args.kwargs["json"] == {
        "grant_type": "client_credentials",
        "client_id": "airflow-sso",
        "client_secret": "s3cret",
    }


def test_password_grant_under_simple_auth(simple_auth, token_response):
    token = airflow_api_client.get_access_token(host="http://airflow-apiserver:8080")

    assert token == "abc123"
    assert token_response.call_args.kwargs["json"] == {
        "username": "airflow",
        "password": "airflow",
    }


def test_get_access_token_rejected(mocker, keycloak_auth):
    mocker.patch.object(
        airflow_api_client.httpx,
        "post",
        return_value=httpx.Response(
            403, text="Client credentials authentication failed"
        ),
    )

    with pytest.raises(RuntimeError, match="Failed to get access token: 403"):
        airflow_api_client.get_access_token(host="http://airflow-apiserver:8080")


def test_get_access_token_connection_error(mocker, keycloak_auth, caplog):
    mocker.patch.object(
        airflow_api_client.httpx,
        "post",
        side_effect=httpx.ConnectError("no route to host"),
    )

    with pytest.raises(httpx.ConnectError):
        airflow_api_client.get_access_token(host="http://airflow-apiserver:8080")

    assert "Connection error: no route to host" in caplog.text


def test_api_client(keycloak_auth, token_response):
    client = airflow_api_client.api_client()

    assert client.configuration.host == "http://airflow-apiserver:8080"
    assert client.configuration.access_token == "abc123"
