from airflow.plugins_manager import AirflowPlugin

from libsys_airflow.plugins.digital_bookplates.apps.digital_bookplates_batch_upload_view import (
    app as digital_bookplates_batch_upload_app,
)
from libsys_airflow.plugins.digital_bookplates.apps.digital_bookplates_download_view import (
    app as digital_bookplates_download_app,
)

digital_bookplates_batch_upload_fastapi_app = {
    "app": digital_bookplates_batch_upload_app,
    "url_prefix": "/digital_bookplates_batch_upload",
    "name": "Digital Bookplates Batch Upload",
}
digital_bookplates_batch_upload_view = {
    "name": "Digital Bookplates Batch Upload",
    "category": "FOLIO",
    "href": "/digital_bookplates_batch_upload/",
    "url_route": "digital_bookplates_batch_upload",
}

digital_bookplates_download_fastapi_app = {
    "app": digital_bookplates_download_app,
    "url_prefix": "/digital_bookplates_download",
    "name": "Digital Bookplates File Download",
}
digital_bookplates_download_view = {
    "name": "Digital Bookplates File Download",
    "category": "FOLIO",
    "href": "/digital_bookplates_download/",
    "url_route": "digital_bookplates_download",
}


class DigitalBookplatesBatchUploadPlugin(AirflowPlugin):
    name = "Digital Bookplates Batch Upload"
    fastapi_apps = [digital_bookplates_batch_upload_fastapi_app]
    external_views = [digital_bookplates_batch_upload_view]


class DigitalBookplatesDownloadPlugin(AirflowPlugin):
    name = "Digital Bookplates File Download"
    fastapi_apps = [digital_bookplates_download_fastapi_app]
    external_views = [digital_bookplates_download_view]
