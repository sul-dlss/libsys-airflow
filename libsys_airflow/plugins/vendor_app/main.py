from airflow.plugins_manager import AirflowPlugin

from libsys_airflow.plugins.vendor_app.vendor_management import app

vendor_management_fastapi_app = {
    "app": app,
    "url_prefix": "/vendor_management",
    "name": "Vendor Management",
}

vendor_management_view = {
    "name": "Dashboard",
    "category": "Vendor Management",
    "href": "/vendor_management/",
    "url_route": "vendor_management",
}


class VendorManagementPlugin(AirflowPlugin):
    name = "Vendor Management"
    fastapi_apps = [vendor_management_fastapi_app]
    external_views = [vendor_management_view]
