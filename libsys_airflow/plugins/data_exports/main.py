from airflow.plugins_manager import AirflowPlugin

from libsys_airflow.plugins.data_exports.apps.data_export_upload_view import (
    app as data_export_upload_app,
)
from libsys_airflow.plugins.data_exports.apps.data_export_download_view import (
    app as data_export_download_app,
)
from libsys_airflow.plugins.data_exports.apps.data_export_oclc_reports_view import (
    app as data_export_oclc_reports_app,
)

data_export_upload_fastapi_app = {
    "app": data_export_upload_app,
    "url_prefix": "/data_export_upload",
    "name": "Data Export CSV Upload",
}
data_export_upload_view = {
    "name": "Data Export CSV Upload",
    "category": "FOLIO",
    "href": "/data_export_upload/",
    "url_route": "data_export_upload",
}

data_export_download_fastapi_app = {
    "app": data_export_download_app,
    "url_prefix": "/data_export_download",
    "name": "Data Export MARC Download",
}
data_export_download_view = {
    "name": "Data Export MARC Download",
    "category": "FOLIO",
    "href": "/data_export_download/",
    "url_route": "data_export_download",
}

data_export_oclc_reports_fastapi_app = {
    "app": data_export_oclc_reports_app,
    "url_prefix": "/data_export_oclc_reports",
    "name": "Data Export OCLC Reports",
}
data_export_oclc_reports_view = {
    "name": "Data Export OCLC Reports",
    "category": "FOLIO",
    "href": "/data_export_oclc_reports/",
    "url_route": "data_export_oclc_reports",
}


class DataExportUploadPlugin(AirflowPlugin):
    name = "Data Export CSV Upload"
    fastapi_apps = [data_export_upload_fastapi_app]
    external_views = [data_export_upload_view]


class DataExportDownloadPlugin(AirflowPlugin):
    name = "Data Export MARC Download"
    fastapi_apps = [data_export_download_fastapi_app]
    external_views = [data_export_download_view]


class DataExportOCLCReportsPlugin(AirflowPlugin):
    name = "Data Export OCLC Reports"
    fastapi_apps = [data_export_oclc_reports_fastapi_app]
    external_views = [data_export_oclc_reports_view]
