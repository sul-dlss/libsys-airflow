import pytest
from pytest_mock import MockerFixture
import requests

from plugins.vendor_app.vendors import VendorManagementView
from tests.mocks import mock_okapi_variable, MockFOLIOClient


@pytest.fixture
def mock_okapi_requests(monkeypatch, mocker: MockerFixture):
    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get_vendors")
        get_response.status_code = 200
        get_response.raise_for_status = lambda: {}
        get_response.json = lambda: {
            "organizations": [{"id": "d558cce0-cb4b-4f28-aad3-c94c7084b2e3",
                               "name": "MarcMarcMarc",
                               "code": "MMM"}]}
        return get_response

    monkeypatch.setattr(requests, "get", mock_get)


@pytest.fixture
def mock_get_folio_client(monkeypatch):
    def mock_folio_client():
        folio_client = MockFOLIOClient()
        return folio_client

    monkeypatch.setattr(VendorManagementView, "_folio_client", mock_folio_client)


def test_vendor_management_view(mock_okapi_variable, mock_okapi_requests, mock_get_folio_client):
    vendor_management_app = VendorManagementView()
    assert vendor_management_app
