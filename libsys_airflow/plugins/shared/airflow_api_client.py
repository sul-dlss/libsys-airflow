import os
import airflow_client.client
from pydantic import BaseModel
import httpx
import logging

from airflow.configuration import conf

logger = logging.getLogger(__name__)


class AirflowAccessToken(BaseModel):
    access_token: str


def _token_payload() -> dict:
    """
    Builds the /auth/token request body for the configured auth manager.

    KeycloakAuthManager authenticates the airflow-sso client's service account
    through Keycloak's client_credentials grant; SimpleAuthManager, which we use
    for local development without an identity provider, wants a username and
    password instead.
    """
    if "keycloak" in conf.get("core", "auth_manager", fallback="").lower():
        return {
            "grant_type": "client_credentials",
            "client_id": os.getenv("AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_ID"),
            "client_secret": os.getenv("AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_SECRET"),
        }

    return {
        "username": os.getenv("AIRFLOW_VAR_API_USER", "nausername"),
        "password": os.getenv("AIRFLOW_VAR_API_PASSWORD", "napassword"),
    }


def get_access_token(host: str) -> str:
    url = f"{host}/auth/token"
    logger.info(f"Getting access token from {url}")
    headers = {"Content-Type": "application/json"}
    try:
        response = httpx.post(url, json=_token_payload(), headers=headers)
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
