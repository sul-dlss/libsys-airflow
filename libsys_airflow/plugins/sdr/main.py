from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

from libsys_airflow.plugins.sdr.apps.sdr_missing_barcodes_view import (
    SdrMissingBarcodesView,
)

sdr_missing_barcodes_bp = Blueprint("sdr", __name__, template_folder="templates")

sdr_missing_barcodes_view = SdrMissingBarcodesView()

sdr_missing_barcodes_package = {
    "name": "SDR Missing Barcodes Reports",
    "category": "FOLIO",
    "view": sdr_missing_barcodes_view,
}


class SdrPlugin(AirflowPlugin):
    name = "SDR Reports"
    operators = []  # type: ignore
    flask_blueprints = [sdr_missing_barcodes_bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [sdr_missing_barcodes_package]
