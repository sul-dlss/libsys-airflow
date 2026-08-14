from fastapi.testclient import TestClient
import pytest  # noqa

from libsys_airflow.plugins.authority_control.apps.deletes_upload_view import app

client = TestClient(app)


def test_deletes_upload_view():
    response = client.get("/")

    assert response.status_code == 200


def test_create_upload():
    response = client.post('/upload')
    assert response.status_code == 200

    assert "Missing file upload" in response.text
