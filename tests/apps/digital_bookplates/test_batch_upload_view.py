from airflow.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response
import pytest

from conftest import root_directory

from libsys_airflow.plugins.digital_bookplates.apps.digital_bookplates_batch_upload_view import (
    DigitalBookplatesBatchUploadView
)

@pytest.fixture
def test_airflow_client():
    templates_folder = f"{root_directory}/libsys_airflow/plugins/digital_bookplates/templates"

    app = application.create_app(testing=True)
    app.config['WTF_CSRF_ENABLED'] = False
    app.appbuilder.add_view(DigitalBookplatesBatchUploadView, "DigitalBookplatesBatchUpload", category="FOLIO")
    app.blueprints['DigitalBookplatesBatchUploadView'].template_folder = templates_folder
    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client

class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")
    
def test_digital_bookplates_batch_upload_view(test_airflow_client):
    response = test_airflow_client.get('/digital_bookplates_batch_upload/')

    assert response.status_code == 200