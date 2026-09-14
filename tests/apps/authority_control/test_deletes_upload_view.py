from fastapi.testclient import TestClient
from auth_helpers import authenticated_app_fixture  # noqa
from csrf_helpers import csrf_test_client  # noqa
import pytest  # noqa

from libsys_airflow.plugins.authority_control.apps.deletes_upload_view import app

authenticated = authenticated_app_fixture(app)
client = csrf_test_client(app)


def test_deletes_upload_view():
    response = client.get("/")

    assert response.status_code == 200


def test_create_upload():
    response = client.post('/upload')
    assert response.status_code == 200

    assert "Missing file upload" in response.text


def test_create_upload_without_csrf_token():
    response = TestClient(app).post('/upload')

    assert response.status_code == 403
    assert response.json()["detail"] == "CSRF token missing or invalid"
