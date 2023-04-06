from airflow.models import DagBag, Variable
import pytest
import pydantic
from pytest_mock import MockerFixture
import requests
import folioclient


class MockFolioClient(pydantic.BaseModel):
    okapi_url = "https://okapi-folio.dev.edu"
    password = "abdccde"
    username = "folio_admin"
    okapi_headers = {}
    def __init__(self, *args, **kwargs):
      breakpoint()
      super().__init__(*args, **kwargs)


@pytest.fixture
def mock_folio_client(monkeypatch):
    monkeypatch.setattr(folioclient, "FolioClient", MockFolioClient)


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


def test_dagbag(mock_aeon_variable, mock_lobby_variable, mock_folio_client):
    dagbag = DagBag("dags")
    assert not dagbag.import_errors
