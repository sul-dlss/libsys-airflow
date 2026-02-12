from airflow.providers.fab.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response
import pytest

from conftest import root_directory
from libsys_airflow.plugins.data_exports.apps.data_export_upload_view import (
    DataExportUploadView,
)


@pytest.fixture
def test_airflow_client():
    templates_folder = f"{root_directory}/libsys_airflow/plugins/data_exports/templates"

    app = application.create_app(enable_plugins=False)
    app.config['WTF_CSRF_ENABLED'] = False

    with app.app_context():
        app.appbuilder.add_view(
            DataExportUploadView, "DataExport", category="Data export"
        )
        app.blueprints['DataExportUploadView'].template_folder = templates_folder

    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")


def test_upload_view(test_airflow_client):
    response = test_airflow_client.get('/data_export_upload/')
    assert response.status_code == 200

    upload_vendors = response.html.find(id="vendor").find_all('option')
    assert upload_vendors[-1].get_text() == "sharevde"


def test_create_upload(test_airflow_client):
    response = test_airflow_client.post('/data_export_upload/create')
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "Missing Instance UUID File" in alert
