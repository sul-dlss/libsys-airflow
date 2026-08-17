import json

import httpx
import pytest  # noqa

from airflow.sdk import Connection

from libsys_airflow.plugins.google_scanning.caiasoft_api import CaiaSoftAPIWrapper


@pytest.fixture
def mock_caiasoft_connection():
    return Connection(
        conn_id="caiasoft_api",
        conn_type="http",
        host="https://library.caiasoft.com",
        login=None,
        password=None,
        extra=json.dumps({"X-API-Key": "secret-key"}),
    )


def test_courier_manifest_requests_expected_url(mocker, mock_caiasoft_connection):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.caiasoft_api.Connection.get",
        return_value=mock_caiasoft_connection,
    )
    mock_get = mocker.patch(
        "httpx.Client.get",
        return_value=httpx.Response(
            200,
            json={"success": True, "shipment_count": 0, "manifest": []},
            request=httpx.Request("GET", "https://library.caiasoft.com"),
        ),
    )

    result = CaiaSoftAPIWrapper().courier_manifest("20260812", "20260813")

    mock_get.assert_called_once_with(
        "https://library.caiasoft.com/api/couriermanifest/v1/20260812/20260813/GOOGLE",
        timeout=30,
    )
    assert result == {"success": True, "shipment_count": 0, "manifest": []}


def test_courier_manifest_uses_given_courier(mocker, mock_caiasoft_connection):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.caiasoft_api.Connection.get",
        return_value=mock_caiasoft_connection,
    )
    mock_get = mocker.patch(
        "httpx.Client.get",
        return_value=httpx.Response(
            200,
            json={"success": True},
            request=httpx.Request("GET", "https://library.caiasoft.com"),
        ),
    )

    CaiaSoftAPIWrapper().courier_manifest("20260812", "20260813", courier="OTHER")

    mock_get.assert_called_once_with(
        "https://library.caiasoft.com/api/couriermanifest/v1/20260812/20260813/OTHER",
        timeout=30,
    )


def test_courier_manifest_raises_on_http_error(mocker, mock_caiasoft_connection):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.caiasoft_api.Connection.get",
        return_value=mock_caiasoft_connection,
    )
    mocker.patch(
        "httpx.Client.get",
        return_value=httpx.Response(
            500, request=httpx.Request("GET", "https://library.caiasoft.com")
        ),
    )

    with pytest.raises(httpx.HTTPStatusError):
        CaiaSoftAPIWrapper().courier_manifest("20260812", "20260813")

