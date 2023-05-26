from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from libsys_airflow.plugins.vendor_app.vendors import VendorManagementView
from libsys_airflow.plugins.vendor_app.database import Session

vendor_mgt_bp = Blueprint(
    "vendor_management",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/vendor",
)


@vendor_mgt_bp.teardown_app_request
def shutdown_session(exception=None):
    Session.remove()


# Vendor Management
vendor_management_view = VendorManagementView()
vendor_management_view_package = {
    "name": "Dashboard",
    "category": "Vendor Management",
    "view": vendor_management_view,
}


class VendorManagementPlugin(AirflowPlugin):
    name = "Vendor Management"
    operators = []
    flask_blueprints = [vendor_mgt_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [vendor_management_view_package]
    appbuilder_menu_items = []
