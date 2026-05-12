from airflow.providers.fab.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response
import pytest

from conftest import root_directory

from libsys_airflow.plugins.authority_control.apps.deletes_upload_view import (
    AuthorityRecordsDeleteUploadView,
)


@pytest.fixture
def test_airflow_client():
    templates_folder = (
        f"{root_directory}/libsys_airflow/plugins/authority_control/templates"
    )

    app = application.create_app(enable_plugins=False)
    app.config['WTF_CSRF_ENABLED'] = False

    with app.app_context():
        app.appbuilder.add_view(
            AuthorityRecordsDeleteUploadView, "DataExport", category="Data export"
        )
        app.blueprints['AuthorityRecordsDeleteUploadView'].template_folder = (
            templates_folder
        )

    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")


def test_deletes_upload_view(test_airflow_client):
    response = test_airflow_client.get("/authorities_deletes/")

    assert response.status_code == 200


def test_create_upload(test_airflow_client):
    response = test_airflow_client.post('/authorities_deletes/upload')
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "Missing file upload" in alert
