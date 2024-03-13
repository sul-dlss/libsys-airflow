from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from libsys_airflow.plugins.data_exports.apps.data_export_upload_view import DataExportUploadView
from libsys_airflow.plugins.data_exports.apps.data_export_download_view import DataExportDownloadView

data_export_upload_bp = Blueprint("data_export_upload", __name__, template_folder="templates")
data_export_download_bp = Blueprint("data_export_download", __name__, template_folder="templates")

data_export_upload_view = DataExportUploadView()
data_export_upload_view_package = {
    "name": "Data Export CSV Upload",
    "category": "FOLIO",
    "view": data_export_upload_view,
}

data_export_download_view = DataExportDownloadView()
data_export_download_view_package = {
    "name": "Data Export MARC Download",
    "category": "FOLIO",
    "view": data_export_download_view,
}


class DataExportUploadPlugin(AirflowPlugin):
    name = "Data Export CSV Upload"
    operators = []  # type: ignore
    flask_blueprints = [data_export_upload_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [data_export_upload_view_package]
    appbuilder_menu_items = []


class DataExportDownloadPlugin(AirflowPlugin):
    name = "Data Export MARC Download"
    operators = []  # type: ignore
    flask_blueprints = [data_export_download_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [data_export_download_view_package]
    appbuilder_menu_items = []
