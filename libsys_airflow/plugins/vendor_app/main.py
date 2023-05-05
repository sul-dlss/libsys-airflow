from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from libsys_airflow.plugins.vendor_app.vendors import VendorManagementView

vendor_mgt_bp = Blueprint(
    "vendor_management",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/vendor"
)

# Vendor Management
vendor_index_view = VendorManagementView()
vendor_index_view_package = {
    "name": "Vendors",
    "category": "Vendor Management",
    "view": vendor_index_view,
}


class VendorManagementPlugin(AirflowPlugin):
    name = "Vendor Management"
    operators = []
    flask_blueprints = [vendor_mgt_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [vendor_index_view_package]
    appbuilder_menu_items = []
