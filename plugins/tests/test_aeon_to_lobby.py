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
            "username": "aeonuser1",
        },
        {
            "transactionNumber": 1,
            "creationDate": today,
            "username": "aeonuser2",
        },
        {
            "transactionNumber": 2,
            "creationDate": today,
            "username": "aeonuser2",
        }
    ]


# Mock xcom
def mock_xcom_pull_user_data(*args, **kwargs):
    return [
        {
            "username": "aeonuser",
            "lastName": "User",
            "firstName": "Aeon",
            "eMailAddress": "aeonu@mail.edu",
            "phone": "999-999-9999",
            "address": "123 Charm St",
            "address2": "Apt A",
            "city": "Palo Alto",
            "state": "CA",
            "zip": "99999",
            "country": "US",
        }
    ]


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = mock_xcom_pull_user_data


@pytest.fixture
def mock_queue_requests(monkeypatch, mocker: MockerFixture):
    def mock_get_queue_data(*args, **kwargs):
        get_response = mocker.stub()
        get_response.status_code = 200
        get_response.json = mock_aeon_queue_data

        return get_response

    monkeypatch.setattr(requests, "get", mock_get_queue_data)


def test_user_data(mock_queue_requests, mock_aeon_variable):
    from plugins.aeon_to_lobby.aeon import user_data
    user_data = user_data(
        aeon_url="https://aeon.example.com", aeon_key="123", queue_id="1"
    )

    assert lambda: user_data[0] == mock_aeon_queue_data[0]
    assert len(user_data) < 3


def test_aeon_request_params(caplog):
    from plugins.aeon_to_lobby.aeon import aeon_user

    assert "aeon rsponded with" not in caplog.text


def test_find_user_from_request_queue(mock_queue_requests, mock_aeon_variable):
    from plugins.aeon_to_lobby.aeon import user_requests_in_queue
    user_list = user_requests_in_queue(
        aeon_url="https://aeon.example.com", aeon_key="123", entry_key="username", queue_id="1"
    )

    assert user_list == ['aeonuser1', 'aeonuser2', 'aeonuser2']


def test_transform_data(mock_lobby_variable):
    from dags.aeon_to_lobby import transform_data

    lobby_users = transform_data(task_instance=MockTaskInstance)

    assert lobby_users[0]['LastName'] == "User"
    assert lobby_users[0]['CustomFields'][0]['Name'] == "Address (Street)"
    assert lobby_users[0]['CustomFields'][0]['Value'] == "123 Charm St, Apt A"
