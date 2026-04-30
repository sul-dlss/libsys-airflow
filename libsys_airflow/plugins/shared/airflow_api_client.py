import os
import airflow_client.client
from pydantic import BaseModel
import httpx
import logging

logger = logging.getLogger(__name__)

class AirflowAccessToken(BaseModel):
    access_token: str


def get_access_token(
    host: str,
    username: str | None,
    password: str | None,
) -> str:
    url = f"{host}/auth/token"
    logger.info(f"Getting access token from {url}")
    payload = {
        "username": username,
        "password": password,
    }
    headers = {"Content-Type": "application/json"}
    try:
        response = httpx.post(url, json=payload, headers=headers)
        if response.status_code == 201:
            response_success = AirflowAccessToken(**response.json())
        else:
            raise RuntimeError(
            f"Failed to get access token: {response.status_code} {response.text}"
        )
    except httpx.ConnectError as e:
        print(f"Connection error: {e}")
        raise

    return response_success.access_token


def api_client() -> airflow_client.client.ApiClient:
    configuration = airflow_client.client.Configuration(
        host="http://airflow-apiserver:8080",
        username=os.getenv("AIRFLOW_VAR_API_USER", "nausername"),
        password=os.getenv("AIRFLOW_VAR_API_PASSWORD", "napassword"),
    )
    configuration.access_token = get_access_token(
        host=configuration.host,
        username=configuration.username,
        password=configuration.password,
    )
    return airflow_client.client.ApiClient(configuration)
