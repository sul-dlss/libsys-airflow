import logging
import requests

from airflow.models import Variable


def lobby_post(**kwargs):
    lobby_url=Variable.get("lobby_url")
    lobby_key=Variable.get("lobby_key")

    lobby_headers = {
        "Authorization": lobby_key,
        "Content-type": "application/json",
        "Accept": "application/json",
    }
    task_instance = kwargs["task_instance"]
    lobby_users = task_instance.xcom_pull(
        key="return_value", task_ids="transform_aeon_data_to_lobby_json"
    )

    for user in lobby_users:
        response = requests.post(lobby_url, headers=lobby_headers, json=user)

        if response.status_code != 200:
            logging.error(
                "lobbytrack api rsponded with:" f"{response.status_code}, {response.text}"
            )
            response.raise_for_status()

        logging.info(f"{response.status_code}, {response.text}, {user}")
