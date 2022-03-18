from datetime import date
from datetime import datetime
import pytest
import requests

from pytest_mock import MockerFixture
from airflow.models import Variable

from plugins.aeon_to_lobby.aeon import user_requests_in_queue


@pytest.fixture
def mock_aeon_variable(monkeypatch):
    def mock_get(key):
        return "https://stanford.aeon.atlas-sys.com/aeon/api"

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def mock_aeon_queue_data():
    today = date.today()
    midnight = datetime.combine(today, datetime.min.time())
    return [
        {
            "transactionNumber": 0,
            "creationDate": midnight,
            "username": "aeonuser1",
        },
        {
            "transactionNumber": 1,
            "creationDate": midnight,
            "username": "aeonuser2",
        }
    ]


@pytest.fixture
def mock_queue_requests(monkeypatch, mocker: MockerFixture):
    def mock_get_data(*args, **kwargs):
        get_response = mocker.stub(text=mock_aeon_queue_data)
        get_response.status_code = 200

        return get_response

    monkeypatch.setattr(requests, "get", mock_get_data)


def test_find_username_in_request_queue(mock_queue_requests, mock_aeon_variable):
    user_requests_in_queue
