from datetime import datetime
import logging
import objectpath
import requests


def user_data(**kwargs):
    users = []
    queue_users = user_requests_in_queue(**kwargs, entry_key="username")
    # Unique usernames inn queue:
    for username in list(dict.fromkeys(queue_users)):
        user = aeon_user(**kwargs, user=username)
        if "@stanford.edu" not in user[0]["username"]:
            logging.info(f"Adding {user}")
            users.append(user)
        else:
            logging.info(f"Skipping {user}")

    return users


def route_aeon_post(**kwargs):
    responses = []
    for id in user_requests_in_queue(**kwargs, entry_key="transactionNumber"):
        aeon_url = kwargs["aeon_url"]
        queue = kwargs["final_queue"]
        aeon_headers = {"X-AEON-API-KEY": kwargs["aeon_key"], "Accept": "application/json"}

        logging.info(f"Routing transactionNumber {id} : {aeon_url}/Requests/{id}/route")
        logging.info(f'{ "newStatus": {queue} }')
        # return None

        response = requests.post(f"{aeon_url}/Requests/{id}/route", headers=aeon_headers, json={ "newStatus": queue })

        if response.status_code != 200:
            logging.error(f"aeon rsponded with: {response.status_code}, {response.text}")
            return None

        responses.append(response.json())

    return responses


def user_requests_in_queue(**kwargs):
    result = []
    entry_key = kwargs['entry_key']
    data = queue_requests(**kwargs, queue=kwargs['queue_id'])
    today = datetime.today().strftime("%Y-%m-%d")
    tree = objectpath.Tree(data)
    generator = tree.execute(f"$.*[@.creationDate >= '{today}'].{entry_key}")
    # generator = tree.execute(f"$.*[@.creationDate < '{today}'].{entry_key}")
    for entry in generator:
        result.append(entry)

    if len(result) < 1:
        logging.info(f"No {entry_key}s in queue_requests at this time: {today}")

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
