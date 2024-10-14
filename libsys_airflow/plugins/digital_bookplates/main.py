from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from libsys_airflow.plugins.digital_bookplates.apps.digital_bookplates_batch_upload_view import (
    DigitalBookplatesBatchUploadView,
)

digital_bookplates_batch_upload_bp = Blueprint(
    "digital_bookplates_batch_upload", __name__, template_folder="templates"
)

digital_bookplates_batch_upload_view = DigitalBookplatesBatchUploadView()

digital_bookplates_batch_upload_package = {
    "name": "Digital Bookplates Batch Upload",
    "category": "FOLIO",
    "view": digital_bookplates_batch_upload_view,
}


class DigitalBookplatesBatchUploadPlugin(AirflowPlugin):
    name = "Digital Bookplates Batch Upload"
    operators = []  # type: ignore
    flask_blueprints = [digital_bookplates_batch_upload_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [digital_bookplates_batch_upload_package]
    appbuilder_menu_items = []
