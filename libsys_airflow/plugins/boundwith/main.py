from airflow.plugins_manager import AirflowPlugin

from libsys_airflow.plugins.boundwith.boundwith_view import app

boundwith_app = {
    "app": app,
    "url_prefix": "/boundwith",
    "name": "Boundwith CSV Upload",
}

boundwith_view = {
    "name": "Boundwith CSV Upload",
    "category": "FOLIO",
    "href": "/boundwith/",
    "url_route": "boundwith",
}


class BoundwithPlugin(AirflowPlugin):
    name = "Boundwith CSV Upload"
    fastapi_apps = [boundwith_app]
    external_views = [boundwith_view]
