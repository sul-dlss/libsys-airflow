from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from libsys_airflow.plugins.orafin.apps.orafin_files_view import OrafinFilesView

orafin_bp = Blueprint("orafin", __name__, template_folder="templates")

orafin_view = OrafinFilesView()
orafin_view_package = {
    "name": "Orafin Feeder-files and Reports",
    "category": "FOLIO",
    "view": orafin_view,
}


class OrafinPlugin(AirflowPlugin):
    name = "Orafin Files"
    operators = []  # type: ignore
    flask_blueprints = [orafin_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [orafin_view_package]
