import pydantic
import pytest  # noqa

from libsys_airflow.plugins.folio.interface import (
    interface_info
)


class MockFOLIOClient(pydantic.BaseModel):
    okapi_url: str = "https://okapi.edu"
    okapi_headers: dict = {}
    locations: list = []

    def get(self, path):
        if path.endswith("/credentials"):
            return credentials_response
        else:
            return interface_response


credentials_response = {
    "id": "2552dadb-aa4e-4d58-ad99-f09384a53018",
    "username": "my_user",
    "password": "my_password",
    "interfaceId": "588b5c42-8634-4af7-bc9b-5e0116ed96b6"
}

interface_response = {
    'uri': 'ftps://www.gobi3.com',
}


def test_interface_info():
    folio_client = MockFOLIOClient()

    interface_id = "588b5c42-8634-4af7-bc9b-5e0116ed96b6"
    info = interface_info(interface_id=interface_id, folio_client=folio_client)

    assert info['uri'] == "ftps://www.gobi3.com"
    assert info['username'] == "my_user"
    assert info['password'] == "my_password"
