from airflow.plugins_manager import AirflowPlugin

from libsys_airflow.plugins.sdr.apps.sdr_missing_barcodes_view import app

sdr_missing_barcodes_app = {
    "app": app,
    "url_prefix": "/sdr",
    "name": "SDR Missing Barcodes Reports",
}

sdr_missing_barcodes_view = {
    "name": "SDR Missing Barcodes Reports",
    "category": "FOLIO",
    "href": "/sdr/",
    "url_route": "sdr",
}


class SdrPlugin(AirflowPlugin):
    name = "SDR Reports"
    fastapi_apps = [sdr_missing_barcodes_app]
    external_views = [sdr_missing_barcodes_view]
