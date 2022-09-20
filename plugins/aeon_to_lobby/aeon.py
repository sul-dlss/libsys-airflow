from datetime import datetime
import logging
import objectpath
import requests


def user_transaction_data(**kwargs):
    users = []
    queue_users = user_requests_in_queue(**kwargs)
    for user_transaction in queue_users:  # [['aeonuser1@stanford.edu', 0], ['aeonuser1@stanford.edu', 1], ["aesonuser2@gmail.com", 2]]
        users.append(user_transaction[0])

    return users


def filtered_users(**kwargs):
    users = []
    task_instance = kwargs["task_instance"]
    queue_users = task_instance.xcom_pull(
        key="return_value", task_ids="get_user_transaction_data_from_aeon"
    )  # ['aeonuser1@stanford.edu', 'aeonuser1@stanford.edu', 'aesonuser2@gmail.com']
    for user in queue_users:
        if "@stanford.edu" not in user:
            user = aeon_user(**kwargs, user=user)
            logging.info(f"Adding {user}")
            users.append(user)
        else:
            logging.info(f"Skipping {user}")

    return users


def route_aeon_post(**kwargs):
    task_instance = kwargs["task_instance"]
    aeon_data = task_instance.xcom_pull(
        key="return_value", task_ids="get_user_transaction_data_from_aeon"
    )  # [['aeonuser1@stanford.edu', 0], ['aeonuser1@stanford.edu', 1], ["aesonuser2@gmail.com", 2]]
    aeon_url = kwargs["aeon_url"]
    queue = kwargs["final_queue"]
    aeon_headers = {"X-AEON-API-KEY": kwargs["aeon_key"], "Accept": "application/json"}

    responses = []
    for user_transactions in aeon_data:
        id = user_transactions[1]
        logging.info(f"Routing transactionNumber {id} : {aeon_url}/Requests/{id}/route")
        logging.info({"newStatus": {queue}})

        response = requests.post(f"{aeon_url}/Requests/{id}/route", headers=aeon_headers, json={"newStatus": queue})

        if response.status_code != 200:
            logging.error(f"aeon rsponded with: {response.status_code}, {response.text}")
            return None

        responses.append(response.json())

    return responses


def user_requests_in_queue(**kwargs):
    result = []
    data = queue_requests(**kwargs, queue=kwargs['queue_id'])
    today = datetime.today().strftime("%Y-%m-%d")
    tree = objectpath.Tree(data)
    generator = tree.execute(f"$.*[@.creationDate >= '{today}']")

    for entry in generator:
        result.append([entry['username'], entry['transactionNumber']])

    if len(result) < 1:
        logging.info(f"No entries in queue_requests at this time: {today}")

    return result


def aeon_user(**kwargs):
    aeon_user = kwargs["user"]
    aeon_url = kwargs["aeon_url"]

    return aeon_get(**kwargs, url=f"{aeon_url}/Users/{aeon_user}")


def queue_requests(**kwargs):
    queue = kwargs["queue"]
    aeon_url = kwargs["aeon_url"]

    return aeon_get(**kwargs, url=f"{aeon_url}/Queues/{queue}/requests")


def aeon_get(**kwargs):
    aeon_key = kwargs["aeon_key"]
    url = kwargs["url"]
    aeon_headers = {"X-AEON-API-KEY": aeon_key, "Accept": "application/json"}
    response = requests.get(url, headers=aeon_headers)
    logging.info(f"aeon_get: {url} : {response.status_code}")
    logging.info(response.json())

    if response.status_code != 200:
        logging.error(f"aeon rsponded with: {response.status_code}, {response.text}")
        return None

    return response.json()
