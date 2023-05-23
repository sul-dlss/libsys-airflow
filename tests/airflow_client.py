from airflow.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response
import pytest

from conftest import root_directory
from libsys_airflow.plugins.vendor_app.vendors import VendorManagementView


@pytest.fixture
def test_airflow_client():
    """
    A test fixture to start up the Airflow test app with the Vendor Management plugin, and return
    a client for it for interacting with the application at the HTTP level.
    """
    templates_folder = f"{root_directory}/libsys_airflow/plugins/vendor_app/templates"

    app = application.create_app(testing=True)
    app.config['WTF_CSRF_ENABLED'] = False
    app.appbuilder.add_view(
        VendorManagementView, "Vendors", category="Vendor Management"
    )
    app.blueprints['VendorManagementView'].template_folder = templates_folder
    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")
