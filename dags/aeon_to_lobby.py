from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

from plugins.aeon_to_lobby.aeon import user_data
from plugins.aeon_to_lobby.lobbytrack import lobby_post


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def transform_data(*args, **kwargs):
    lobby_users = []
    task_instance = kwargs["task_instance"]
    aeon_users = task_instance.xcom_pull(
        key="return_value", task_ids="get_user_data_from_aeon"
    )

    for aeon_user in aeon_users:
        # map keys and values for user
        user = {
            "CustomFields": [],
            "IsBlocked": False,
            "BlockedReason": None,
            "ReusableBadgeBarcode": None,
        }
        user["FirstName"] = aeon_user["firstName"]
        user["LastName"] = aeon_user["lastName"]
        user["Email"] = aeon_user["eMailAddress"]
        user["Phone"] = aeon_user["phone"]
        user["CustomFields"].append(
            {"Name": "Address (Street)", "Value": aeon_user["adddress"]}
        )
        user["CustomFields"].append({"Name": "City", "Value": aeon_user["city"]})
        user["CustomFields"].append(
            {"Name": "State or Province", "Value": aeon_user["state"]}
        )
        user["CustomFields"].append({"Name": "Zip code", "Value": aeon_user["zip"]})
        user["CustomFields"].append({"Name": "Country", "Value": aeon_user["country"]})
        user["CustomFields"].append(
            {
                "Name": "Desired Library or Collection",
                "Value": aeon_user["Special Collections"],
            }
        )

        lobby_users.append(user)

    return lobby_users


with DAG(
    "aeon_to_lobbytrack_visitor",
    default_args=default_args,
    schedule_interval=timedelta(hours=12),
    start_date=datetime(2022, 1, 3),
    catchup=False,
    tags=["aeon_to_lobbytrack"],
) as dag:

    aeon_user_data = PythonOperator(
        task_id="get_user_data_from_aeon", python_callable=user_data
    )

    transform_to_lobby_data = PythonOperator(
        task_id="transform_aeon_data_to_lobby_json", python_callable=transform_data
    )

    post_to_lobbytrack = PythonOperator(
        task_id="post_to_lobbytrack", python_callable=lobby_post
    )


aeon_user_data >> transform_to_lobby_data >> post_to_lobbytrack
