from datetime import datetime
import logging
import objectpath
import requests


def user_data(**kwargs):
    users = []
    for username in user_requests_in_queue(**kwargs):
        user = aeon_user(**kwargs, user=username)
        users.append(user)

    return users


def user_requests_in_queue(**kwargs):
    usernames = []
    data = queue_requests(**kwargs, id=1)
    today = datetime.today().strftime("%Y-%m-%d")
    tree = objectpath.Tree(data)
    result = tree.execute(f"$.*[@.creationDate >= '{today}'].username")
    for entry in result:
        usernames.append(entry)

    return usernames


def aeon_user(**kwargs):
    aeon_user = kwargs["user"]
    aeon_url = kwargs["aeon_url"]

    return aeon_request(**kwargs, url=f"{aeon_url}/Users/{aeon_user}")


def queue_requests(**kwargs):
    queue = kwargs["id"]
    aeon_url = kwargs["aeon_url"]

    return aeon_request(**kwargs, url=f"{aeon_url}/Queues/{queue}/requests")


def aeon_request(**kwargs):
    aeon_key = kwargs["aeon_key"]
    aeon_headers = {"X-AEON-API-KEY": aeon_key, "Accept": "application/json"}
    response = requests.get(kwargs["url"], headers=aeon_headers)

    if response.status_code != 200:
        logging.error(f"aeon rsponded with: {response.status_code}, {response.text}")
        return None

    return response.json()
