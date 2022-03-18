from datetime import datetime
import pytest
import requests

from pytest_mock import MockerFixture
from airflow.models import Variable


@pytest.fixture
def mock_aeon_variable(monkeypatch):
    def mock_get(key):
        return "https://stanford.aeon.atlas-sys.com/aeon/api"

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
        }
    ]


@pytest.fixture
def mock_queue_requests(monkeypatch, mocker: MockerFixture):
    def mock_get_data(*args, **kwargs):
        get_response = mocker.stub()
        get_response.status_code = 200
        get_response.json = mock_aeon_queue_data

        return get_response

    monkeypatch.setattr(requests, "get", mock_get_data)


def test_find_username_in_request_queue(mock_queue_requests, mock_aeon_variable):
    from plugins.aeon_to_lobby.aeon import user_requests_in_queue
    user_list = user_requests_in_queue()

    print(f"HERE: {user_list}")