from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from libsys_airflow.plugins.folio.apps.circ_rules_tester_view import CircRulesTester
from libsys_airflow.plugins.folio.apps.healthcheck_view import Healthcheck
from libsys_airflow.plugins.folio.apps.folio_migration_view import FOLIOMigrationReports


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
    "view": circ_rules_tester_view,
}

# Healthcheck App
healthcheck_view = Healthcheck()
healthcheck_package = {
    "name": "Healthcheck",
    "category": "FOLIO",
    "view": healthcheck_view,
}


class FOLIOPlugin(AirflowPlugin):
    name = "FOLIOInformation"
    operators = []  # type: ignore
    flask_blueprints = [bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [
        folio_appbuilder_package,
        circ_rules_tester_package,
        healthcheck_package,
    ]
    appbuilder_menu_items = []
