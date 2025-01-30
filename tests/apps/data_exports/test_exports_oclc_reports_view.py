from airflow.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response
import pytest

from conftest import root_directory
from libsys_airflow.plugins.data_exports.apps.data_export_oclc_reports_view import (
    DataExportOCLCReportsView,
)


@pytest.fixture
def test_airflow_client():
    templates_folder = f"{root_directory}/libsys_airflow/plugins/data_exports/templates"
    files_base = f"{root_directory}/tests/apps/data_exports/data_export_file_fixtures"

    app = application.create_app(testing=True)
    app.config['WTF_CSRF_ENABLED'] = False
    setattr(DataExportOCLCReportsView, "files_base", files_base)  # noqa
    app.appbuilder.add_view(
        DataExportOCLCReportsView, "DataExport", category="Data export"
    )
    app.blueprints['DataExportOCLCReportsView'].template_folder = templates_folder
    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")


def test_oclc_reports_view(test_airflow_client):
    response = test_airflow_client.get('/data_export_oclc_reports/')
    assert response.status_code == 200

    lib_divs = response.html.find_all("div", {"class": "oclc"})

    assert (lib_divs[0].find("h3").get_text()) == "Graduate School of Business"
    assert (
        (lib_divs[0].find("a").get("href"))
        == "/data_export_oclc_reports/S7Z/new_marc_errors/2024-09-13T16:01:18.963349.html"
    )

    assert (lib_divs[1].find("h3").get_text()) == "Stanford University Libraries"
    assert (
        (lib_divs[1].find("a").get("href"))
        == "/data_export_oclc_reports/STF/unset_holdings/2024-09-13T15:47:28.857056.html"
    )
