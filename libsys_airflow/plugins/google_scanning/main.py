from airflow.plugins_manager import AirflowPlugin

from libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view import (
    app,
)

google_scanning_upload_app = {
    "app": app,
    "url_prefix": "/google_scanning",
    "name": "Google Scanning Upload",
}

google_scanning_upload_view = {
    "name": "Google Scanning Upload",
    "category": "FOLIO",
    "href": "/google_scanning/",
    "url_route": "google_scanning",
}


class GoogleScanningPlugin(AirflowPlugin):
    name = "google_scanning"
    fastapi_apps = [google_scanning_upload_app]
    external_views = [google_scanning_upload_view]
