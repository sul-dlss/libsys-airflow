from airflow.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response
import pytest

from conftest import root_directory
from libsys_airflow.plugins.digital_bookplates.apps.digital_bookplates_download_view import (
    DigitalBookplatesDownloadView,
)


@pytest.fixture
def test_airflow_client():
    templates_folder = (
        f"{root_directory}/libsys_airflow/plugins/digital_bookplates/templates"
    )
    files_base = f"{root_directory}/tests/apps/digital_bookplates/digital_bookplates_file_fixtures"

    app = application.create_app(testing=True)
    app.config['WTF_CSRF_ENABLED'] = False
    setattr(DigitalBookplatesDownloadView, "files_base", files_base)
    app.appbuilder.add_view(
        DigitalBookplatesDownloadView,
        "DigitalBookplates",
        category="Digital bookplates",
    )
    app.blueprints['DigitalBookplatesDownloadView'].template_folder = templates_folder
    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")


def test_download_view(test_airflow_client):
    response = test_airflow_client.get('/digital_bookplates_download/')
    assert response.status_code == 200

    csv_download = response.html.find(class_="2024-10-22").get('href')
    assert (
        csv_download
        == "/digital_bookplates_download/2024/10/22/SearchInstanceUUIDs2024-10-16T16_39_11-06_00.csv"
    )
