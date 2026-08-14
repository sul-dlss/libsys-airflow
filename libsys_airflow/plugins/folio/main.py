from airflow.plugins_manager import AirflowPlugin

from libsys_airflow.plugins.folio.apps.circ_rules_tester_view import (
    app as circ_rules_tester_app,
)

circ_rules_tester_fastapi_app = {
    "app": circ_rules_tester_app,
    "url_prefix": "/circ_rule_tester",
    "name": "Circ Rules Tester",
}
circ_rules_tester_view = {
    "name": "Circ Rules Tester",
    "category": "FOLIO",
    "href": "/circ_rule_tester/",
    "url_route": "circ_rule_tester",
}


class FOLIOPlugin(AirflowPlugin):
    name = "FOLIOInformation"
    fastapi_apps = [circ_rules_tester_fastapi_app]
    external_views = [circ_rules_tester_view]
