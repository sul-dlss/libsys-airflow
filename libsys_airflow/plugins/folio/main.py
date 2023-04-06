from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from plugins.folio.apps.circ_rules_tester_view import CircRulesTester
from plugins.folio.apps.folio_migration_view import FOLIOMigrationReports


bp = Blueprint(
    "folio_plugin",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/folio_plugin",
)

# FOLIO Migration Reports
folio_appbuilder_view = FOLIOMigrationReports()
folio_appbuilder_package = {
    "name": "FOLIO Reports and Logs",
    "category": "FOLIO",
    "view": folio_appbuilder_view,
}

# Circ Rules Tester App
circ_rules_tester_view = CircRulesTester()
circ_rules_tester_package = {
    "name": "Circ Rules Tester",
    "category": "FOLIO",
    "view": circ_rules_tester_view
}


class FOLIOPlugin(AirflowPlugin):
    name = "FOLIOInformation"
    operators = []
    flask_blueprints = [bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [folio_appbuilder_package, circ_rules_tester_package]
    appbuilder_menu_items = []
