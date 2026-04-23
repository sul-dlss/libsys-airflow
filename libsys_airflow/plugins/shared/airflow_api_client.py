from airflow.sdk import Variable
import airflow_client.client
from pydantic import BaseModel
import httpx


class AirflowAccessToken(BaseModel):
    access_token: str


def client_configuration() -> airflow_client.client.Configuration:
    return airflow_client.client.Configuration(
        host="http://localhost:8080",
        username=Variable.get("AIRFLOW_VAR_API_USER", "nausername"),
        password=Variable.get("AIRFLOW_VAR_API_PASSWORD", "napassword"),
    )


def get_access_token(
    host: str,
    username: str,
    password: str,
) -> str:
    url = f"{host}/auth/token"
    payload = {
        "username": username,
        "password": password,
    }
    headers = {"Content-Type": "application/json"}
    response = httpx.post(url, json=payload, headers=headers)
    if response.status_code != 201:
        raise RuntimeError(
            f"Failed to get access token: {response.status_code} {response.text}"
        )
    response_success = AirflowAccessToken(**response.json())
    return response_success.access_token


def api_client(
    configuration: airflow_client.client.Configuration,
) -> airflow_client.client.ApiClient:
    return airflow_client.client.ApiClient(configuration)
