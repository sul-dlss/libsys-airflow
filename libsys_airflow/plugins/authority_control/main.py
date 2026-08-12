from airflow.plugins_manager import AirflowPlugin

from libsys_airflow.plugins.authority_control.apps.deletes_upload_view import app

authority_deletes_upload_app = {
    "app": app,
    "url_prefix": "/delete_authority_records",
    "name": "FOLIO Authority Deletes Upload",
}

authority_deletes_view = {
    "name": "FOLIO Authority Deletes Upload",
    "category": "FOLIO",
    "href": "/delete_authority_records/",
    "url_route": "delete_authority_records",
}


class AuthorityDeletesPlugin(AirflowPlugin):
    name = "FOLIO Authority Deletes Upload"
    fastapi_apps = [authority_deletes_upload_app]
    external_views = [authority_deletes_view]
