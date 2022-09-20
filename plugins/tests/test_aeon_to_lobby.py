from datetime import datetime
import pydantic
import pytest
import requests

from pytest_mock import MockerFixture
from airflow.models import Variable


@pytest.fixture
def mock_aeon_variable(monkeypatch):
    def mock_get(key):
        return "https://stanford.aeon.atlas-sys.com/aeon/api"

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def mock_lobby_variable(monkeypatch):
    def mock_get(key):
        return "https://api.lobbytrack.com/api/v1/visitors"

    monkeypatch.setattr(Variable, "get", mock_get)


def mock_aeon_queue_data():
    today = datetime.today().strftime("%Y-%m-%d")
    return [
        {
            "transactionNumber": 0,
            "creationDate": today,
            "username": "aeonuser1@stanford.edu",
        },
        {
            "transactionNumber": 1,
            "creationDate": today,
            "username": "aeonuser1@stanford.edu",
        },
        {
            "transactionNumber": 2,
            "creationDate": today,
            "username": "aeonuser2@gmail.com",
        }
    ]


def mock_aeon_route_post_response():
    return {"transactionNumber": 2, "username": "aeonuser2@gmail.com"}


def mock_xcom_pull_user_data(**kwargs):
    return [
        {
            "username": "aeonuser2@gmail.com",
            "lastName": "User",
            "firstName": "Aeon",
            "eMailAddress": "aeonuser2@gmail.com",
            "phone": "999-999-9999",
            "address": "123 Charm St",
            "address2": "Apt A",
            "city": "Palo Alto",
            "state": "CA",
            "zip": "99999",
            "country": "US",
        }
    ]


def mock_xcom_pull_aeon_users(**kwargs):
    return ['aeonuser1@stanford.edu', 'aeonuser1@stanford.edu', 'aesonuser2@gmail.com']


def mock_xcom_pull_aeon_users_transactions(**kwargs):
    return [['aeonuser1@stanford.edu', 0], ['aeonuser1@stanford.edu', 1], ['aesonuser2@gmail.com', 2]]


class MockTaskInstanceUserData(pydantic.BaseModel):
    xcom_pull = mock_xcom_pull_user_data


class MockTaskInstanceAeonUsers(pydantic.BaseModel):
    xcom_pull = mock_xcom_pull_aeon_users


class MockTaskInstanceAeonUsersTransactions(pydantic.BaseModel):
    xcom_pull = mock_xcom_pull_aeon_users_transactions


@pytest.fixture
def mock_queue_requests(monkeypatch, mocker: MockerFixture):
    def mock_get_queue_data(*args, **kwargs):
        get_response = mocker.stub()
        get_response.status_code = 200
        get_response.json = mock_aeon_queue_data

        return get_response

    monkeypatch.setattr(requests, "get", mock_get_queue_data)


@pytest.fixture
def mock_aeon_post(monkeypatch, mocker: MockerFixture):
    def mock_post_transaction_routing(*args, **kwargs):
        post_response = mocker.stub()
        post_response.status_code = 200
        post_response.json = mock_aeon_route_post_response

        return post_response

    monkeypatch.setattr(requests, "post", mock_post_transaction_routing)


def test_user_data(mock_queue_requests, mock_aeon_variable):
    from plugins.aeon_to_lobby.aeon import user_transaction_data
    user_data = user_transaction_data(
        aeon_url="https://aeon.example.us", aeon_key="123", queue_id="1"
    )

    # assert lambda: user_data[0] == mock_aeon_queue_data[0]
    assert lambda: user_data[0] == ["aeonuser2@gmail.com", 2]
    assert len(user_data) == 3


def test_filtered_user_data(mock_queue_requests, mock_aeon_variable):
    from plugins.aeon_to_lobby.aeon import filtered_users
    filtered_users = filtered_users(
        aeon_url="https://aeon.example.us", aeon_key="123", task_instance=MockTaskInstanceAeonUsers
    )

    for user in filtered_users:
        assert "@stanford.edu" not in user


def test_find_user_from_request_queue(mock_queue_requests, mock_aeon_variable):
    from plugins.aeon_to_lobby.aeon import user_requests_in_queue
    user_list = user_requests_in_queue(
        aeon_url="https://aeon.example.us", aeon_key="123", queue_id="1"
    )

    assert user_list == [['aeonuser1@stanford.edu', 0], ['aeonuser1@stanford.edu', 1], ['aeonuser2@gmail.com', 2]]


def test_transform_data(mock_lobby_variable):
    from dags.aeon_to_lobby import transform_data

    lobby_users = transform_data(task_instance=MockTaskInstanceUserData)

    assert lobby_users[0]['LastName'] == "User"
    assert lobby_users[0]['Email'] == "aeonuser2@gmail.com"
    assert lobby_users[0]['CustomFields'][0]['Name'] == "Address (Street)"
    assert lobby_users[0]['CustomFields'][0]['Value'] == "123 Charm St, Apt A"


def test_route_aeon_post(mock_queue_requests, mock_aeon_variable, mock_aeon_post, caplog):
    from plugins.aeon_to_lobby.aeon import route_aeon_post
    route_aeon_post(
        aeon_url="https://aeon.example.us",
        aeon_key="123",
        queue_id="1",
        final_queue="Awaiting Request Processing",
        task_instance=MockTaskInstanceAeonUsersTransactions
    )

    assert "{'newStatus': {'Awaiting Request Processing'}}" in caplog.text
    assert "aeon rsponded with" not in caplog.text
