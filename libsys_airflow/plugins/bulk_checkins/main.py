from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from libsys_airflow.plugins.bulk_checkins.apps.bulk_barcodes_checkins_view import (
    BulkCheckinsUploadView,
)

bulk_checkins_upload_bp = Blueprint(
    "bulk_checkins", __name__, template_folder="templates"
)

bulk_checkins_upload_view = BulkCheckinsUploadView()

bulk_checkins_upload_view_package = {
    "name": "Bulk Checkins Upload",
    "category": "FOLIO",
    "view": bulk_checkins_upload_view,
}


class BulkCheckinsUploadPlugin(AirflowPlugin):
    name = "Bulk Checkins Upload"
    operators = []  # type: ignore
    flask_blueprints = [bulk_checkins_upload_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [bulk_checkins_upload_view_package]
    appbuilder_menu_items = []
