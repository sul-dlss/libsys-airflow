import pathlib

import pytest
from airflow.www import app as application

from libsys_airflow.plugins.vendor_app.vendors import VendorManagementView


@pytest.fixture
def test_client():
    """
    Start up the Airflow test app with the Vendor Management plugin, and return
    a client for it.
    """
    root_directory = pathlib.Path(__file__).parent.parent
    templates_folder = f"{root_directory}/libsys_airflow/plugins/vendor_app/templates"

    app = application.create_app(testing=True)
    app.appbuilder.add_view(VendorManagementView, "Vendors", category="Vendor Management")
    app.blueprints['VendorManagementView'].template_folder = templates_folder

    with app.test_client() as client:
        yield client
