from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from libsys_airflow.plugins.saml.apps.saml_view import SamlView

saml_bp = Blueprint("saml", __name__, template_folder="templates")

saml_view = SamlView()

saml_package = {"name": "SAML", "category": "FOLIO", "view": saml_view}


class SamlPlugin(AirflowPlugin):
    name = "SAML"
    operators = []  # type: ignore
    flask_blueprints = [saml_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [saml_package]
    appbuilder_menu_items = []
