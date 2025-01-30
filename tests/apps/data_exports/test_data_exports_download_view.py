from airflow.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response
import pytest

from conftest import root_directory
from libsys_airflow.plugins.data_exports.apps.data_export_download_view import (
    DataExportDownloadView,
)


@pytest.fixture
def test_airflow_client():
    templates_folder = f"{root_directory}/libsys_airflow/plugins/data_exports/templates"
    files_base = f"{root_directory}/tests/apps/data_exports/data_export_file_fixtures"

    app = application.create_app(testing=True)
    app.config['WTF_CSRF_ENABLED'] = False
    setattr(DataExportDownloadView, "files_base", files_base)  # noqa
    app.appbuilder.add_view(
        DataExportDownloadView, "DataExport", category="Data export"
    )
    app.blueprints['DataExportDownloadView'].template_folder = templates_folder
    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")


def test_download_view(test_airflow_client):
    response = test_airflow_client.get('/data_export_download/')
    assert response.status_code == 200

    marc_new_download = response.html.find(class_="oclc-marc-files-new").get('href')
    assert (
        marc_new_download
        == "/data_export_download/downloads/oclc/marc-files/new/202003131720.mrc"
    )

    transmitted_deletes_download = response.html.find(
        class_="oclc-transmitted-deletes"
    ).get('href')
    assert (
        transmitted_deletes_download
        == "/data_export_download/downloads/oclc/transmitted/deletes/202103131720.mrc"
    )
