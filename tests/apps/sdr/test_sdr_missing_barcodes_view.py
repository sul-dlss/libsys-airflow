import pathlib

import pytest


from airflow.providers.fab.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response

from conftest import root_directory

from libsys_airflow.plugins.sdr.apps.sdr_missing_barcodes_view import (
    SdrMissingBarcodesView,
)


@pytest.fixture
def test_airflow_client():
    templates_folder = f"{root_directory}/libsys_airflow/plugins/sdr/templates"
    reports_base = pathlib.Path(f"{root_directory}/tests/apps/sdr/report_file_fixtures")

    app = application.create_app(enable_plugins=False)

    app.config['WTF_CSRF_ENABLED'] = False
    setattr(SdrMissingBarcodesView, "reports_base", reports_base)  # noqa

    with app.app_context():
        app.appbuilder.add_view(
            SdrMissingBarcodesView, "SDRReports", category="SDR Report"
        )
        app.blueprints['SdrMissingBarcodesView'].template_folder = templates_folder

    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")


def test_sdr_missing_barcodes_view(test_airflow_client):
    response = test_airflow_client.get('/sdr/')
    assert response.status_code == 200
    report_link = response.html.find(download="")
    assert report_link.text.startswith("missing-barcodes")
