import pathlib

from airflow.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response
import pytest

from conftest import root_directory

from libsys_airflow.plugins.orafin.apps.orafin_files_view import OrafinFilesView


@pytest.fixture
def test_airflow_client():
    templates_folder = f"{root_directory}/libsys_airflow/plugins/orafin/templates"
    files_base = pathlib.Path(
        f"{root_directory}/tests/apps/orafin/orafin_file_fixtures"
    )

    app = application.create_app(testing=True)
    app.config['WTF_CSRF_ENABLED'] = False
    setattr(OrafinFilesView, "files_base", files_base)
    app.appbuilder.add_view(OrafinFilesView, "Orafin", category="Folio")
    app.blueprints['OrafinFilesView'].template_folder = templates_folder
    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")


def test_orafin_files_home(test_airflow_client):
    response = test_airflow_client.get("/orafin/")
    assert response.status_code == 200

    feeder_file_links = response.html.select("#feederFileDownloads tbody tr td a")

    assert feeder_file_links[0].attrs['href'] == "/orafin/data/feeder20241130_20241218"

    ap_report_links = response.html.select("#apReportsDownloads tbody tr td a")

    assert len(ap_report_links) == 3


def test_downloads(test_airflow_client):
    response = test_airflow_client.get("/orafin/data/feeder20241130_20241218")
    assert response.status_code == 200

    assert response.mimetype.startswith("application/text")

    csv_response = test_airflow_client.get(
        "/orafin/reports/xxdl_ap_payments_1218202453000.csv"
    )

    assert csv_response.mimetype.startswith("application/csv")
