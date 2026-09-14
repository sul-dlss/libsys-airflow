import os
import airflow_client.client
from pydantic import BaseModel
import httpx
import logging

from airflow.configuration import conf

logger = logging.getLogger(__name__)


class AirflowAccessToken(BaseModel):
    access_token: str


def _using_keycloak() -> bool:
    return "keycloak" in conf.get("core", "auth_manager", fallback="").lower()


def _token_request(url: str) -> httpx.Response:
    """
    Requests a token from /auth/token for the configured auth manager.

    KeycloakAuthManager authenticates the airflow-sso client's service account
    through Keycloak's client_credentials grant. Local development runs
    SimpleAuthManager with simple_auth_manager_all_admins, where the GET form of
    the endpoint hands out an anonymous admin token without any credentials.
    """
    if _using_keycloak():
        return httpx.post(
            url,
            json={
                "grant_type": "client_credentials",
                "client_id": os.getenv("AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_ID"),
                "client_secret": os.getenv(
                    "AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_SECRET"
                ),
            },
            headers={"Content-Type": "application/json"},
        )

    return httpx.get(url)


def get_access_token(host: str) -> str:
    url = f"{host}/auth/token"
    logger.info(f"Getting access token from {url}")
    try:
        response = _token_request(url)
        if response.status_code == 201:
            response_success = AirflowAccessToken(**response.json())
        else:
            raise RuntimeError(
                f"Failed to get access token: {response.status_code} {response.text}"
            )
    except httpx.ConnectError as e:
        logger.error(f"Connection error: {e}")
        raise

    return response_success.access_token


def api_client() -> airflow_client.client.ApiClient:
    configuration = airflow_client.client.Configuration(
        host="http://airflow-apiserver:8080",
    )
    configuration.access_token = get_access_token(host=configuration.host)
    return airflow_client.client.ApiClient(configuration)
