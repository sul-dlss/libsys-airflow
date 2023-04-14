from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from plugins.data_loading_management.data_loading import DataManagementView
from plugins.data_loading_management.vendors import VendorsView

data_load_mgt_bp = Blueprint(
    "data_loading_management",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/data_loading_management"
)

# Data management
data_mgt_view = DataManagementView()
data_mg_view_package = {
    "name": "Data Loading Home",
    "category": "Data Loading Management",
    "view": data_mgt_view
}

# Vendor Management
vendor_index_view = VendorsView()
vendor_index_view_package = {
    "name": "Vendor Management",
    "category": "Data Loading Management",
    "view": vendor_index_view,
}



class VendorDataManagementPlugin(AirflowPlugin):
    name = "DataLoadingManagement"
    operators = []
    flask_blueprints = [data_load_mgt_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [data_mg_view_package, vendor_index_view_package]
    appbuilder_menu_items = []