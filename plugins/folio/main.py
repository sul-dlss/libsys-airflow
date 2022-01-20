from airflow.plugins_manager import AirflowPlugin


migration_reports = {
    "category": 'FOLIO',
    "name": "Migration Reports",
    "href": "#"
}

instance_logs = {
    "category": 'FOLIO',
    "name": "Instance Logs",
    "href": "#"
}

holding_logs = {
    "category": 'FOLIO',
    "name": "Holding Logs",
    "href": "#"
}


class FOLIOPlugin(AirflowPlugin):
    name = "FOLIOInformation"
    operators = []
    flask_blueprints = []
    hooks = []
    executors = []
    admin_views = []
    appbuilder_menu_items = [migration_reports, instance_logs, holding_logs]