from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from libsys_airflow.plugins.data_exports.data_export_upload_view import (
    DataExportUploadView,
)

fileupload_bp = Blueprint("data_export_upload", __name__, template_folder="templates")

data_export_upload_view = DataExportUploadView()
data_export_upload_view_package = {
    "name": "Data Export CSV Upload",
    "category": "FOLIO",
    "view": data_export_upload_view,
}


class DataExportUploadPlugin(AirflowPlugin):
    name = "Data Export CSV Upload"
    operators = []  # type: ignore
    flask_blueprints = [fileupload_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [data_export_upload_view_package]
    appbuilder_menu_items = []
