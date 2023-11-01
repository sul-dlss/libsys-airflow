from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from libsys_airflow.plugins.boundwith.boundwith_view import BoundWithView

boundwith_bp = Blueprint("boundwith_upload", __name__, template_folder="templates")

boundwith_view = BoundWithView()
boundwith_view_package = {
    "name": "Boundwith CSV Upload",
    "category": "FOLIO",
    "view": boundwith_view,
}


class BoundwithPlugin(AirflowPlugin):
    name = "Boundwith CSV Upload"
    operators = []  # type: ignore
    flask_blueprints = [boundwith_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [boundwith_view_package]
    appbuilder_menu_items = []
