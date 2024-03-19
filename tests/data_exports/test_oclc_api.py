import httpx
import pymarc
import pytest

from unittest.mock import MagicMock

# from libsys_airflow.plugins.data_exports.oclc_api import OCLCAPIWrapper
from libsys_airflow.plugins.data_exports import oclc_api

@pytest.fixture
def mock_httpx(mocker):
    def mock_get(*args, **kwargs):
        return {}

    def mock_post(*args, **kwargs):
        post_result = mocker
        
        if kwargs['url'].startswith("https://oauth.oclc.org/"):
            post_result.json = lambda: {'access_token': "abcded12345"}
        return post_result

    def mock_put(*args, **kwargs):
        return {}

    mock = MagicMock()
    mock.get = mock_get
    mock.post = mock_post
    mock.put = mock_put
    return mock

@pytest.fixture
def mock_folio_client(mocker):
    mock = mocker
    mock.okapi_headers = {}
    mock.okapi_url = "https://okapi.stanford.edu/"
    return mock



def test_oclc_wrapper_class_init(mocker,mock_httpx, mock_folio_client):
    mocker.patch.object(
        oclc_api,
        "httpx",
        mock_httpx
    )

    mocker.patch(
        "libsys_airflow.plugins.data_exports.oclc_api.folio_client",
        return_value=mock_folio_client
    )
    oclc_api_instance = oclc_api.OCLCAPIWrapper(user="oclc_user", password="oclc_password")

    assert oclc_api_instance.folio_client.okapi_url == "https://okapi.stanford.edu/"
    assert oclc_api_instance.oclc_headers == {
        "Authorization": "abcded12345", 
        "Content-type": "application/marc"
    }