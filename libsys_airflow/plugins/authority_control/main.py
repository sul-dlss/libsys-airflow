from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from libsys_airflow.plugins.authority_control.apps.deletes_upload_view import (
    AuthorityRecordsDeleteUploadView,
)

authority_deletes_upload_bp = Blueprint(
    "authority_deletes", __name__, template_folder="templates"
)

authority_deletes_view = AuthorityRecordsDeleteUploadView()
authority_deletes_view_package = {
    "name": "Authority 001 Deletes Upload",
    "category": "FOLIO",
    "view": authority_deletes_view,
}


class AuthorityDeletesPlugin(AirflowPlugin):
    name = "Authority 001 Deletes Upload"
    operators = []  # type: ignore
    flask_blueprints = [authority_deletes_upload_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [authority_deletes_view_package]
    appbuilder_menu_items = []
