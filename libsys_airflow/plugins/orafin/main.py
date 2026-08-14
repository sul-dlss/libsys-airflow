from airflow.plugins_manager import AirflowPlugin

from libsys_airflow.plugins.orafin.apps.orafin_files_view import app

orafin_files_app = {
    "app": app,
    "url_prefix": "/orafin",
    "name": "Orafin Feeder-files and Reports",
}

orafin_files_view = {
    "name": "Orafin Feeder-files and Reports",
    "category": "FOLIO",
    "href": "/orafin/",
    "url_route": "orafin",
}


class OrafinPlugin(AirflowPlugin):
    name = "Orafin Files"
    fastapi_apps = [orafin_files_app]
    external_views = [orafin_files_view]
