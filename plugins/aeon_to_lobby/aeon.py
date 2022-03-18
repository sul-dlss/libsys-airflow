from datetime import datetime
import logging
import objectpath
import requests

from airflow.models import Variable

AEON_URL = Variable.get("AEON_URL")
AEON_KEY = Variable.get("AEON_KEY")

aeon_headers = {"X-AEON-API-KEY": AEON_KEY, "Accept": "application/json"}


def user_requests_in_queue():
    usernames = []
    data = queue_requests(id=1)
    today = datetime.today().strftime("%Y-%m-%d")
    tree = objectpath.Tree(data)
    result = tree.execute(f"$.*[@.creationDate > '{today}'].username")
    for entry in result:
        usernames.append(entry)

    return usernames


def user_data(*args, **kwargs):
    users = []
    for username in user_requests_in_queue():
        user = aeon_user(user=username)
        users.append(user)

    return users


def aeon_user(**kwargs):
    aeon_user = kwargs["user"]
    return aeon_request(url=f"{AEON_URL}/Users/{aeon_user}")


def queue_requests(**kwargs):
    queue = kwargs["id"]
    return aeon_request(url=f"{AEON_URL}/Queues/{queue}/requests")


def aeon_request(**kwargs):
    response = requests.get(kwargs["url"], headers=aeon_headers)

    if response.status_code != 200:
        logging.error(f"aeon rsponded with: {response.status_code}, {response.text}")
        return None

    return response.json()
