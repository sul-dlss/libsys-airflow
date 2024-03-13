import json
import pathlib
from flask_appbuilder import expose, BaseView as AppBuilderBaseView


APP_BASE = "/opt/airflow/libsys_airflow/plugins/data_exports/apps"
DATA_EXPORT_FILES = "/opt/airflow/data-export-files"

vendor_file = open(f"{APP_BASE}/vendors.json")
vendors = json.load(vendor_file)


class DataExportDownloadView(AppBuilderBaseView):
    default_view = "data_export_download_home"
    route_base = "/data_export_download"

    @expose("/")
    def data_export_download_home(self):
        content = []
        for vendor in vendors['vendors']:
            for path in pathlib.Path(f"{DATA_EXPORT_FILES}/{vendor}/marc-files").glob("*"):
                content.append({vendor: path.name})

        return self.render_template("data-export-download/index.html", content=content)

    @expose("/downloads/<vendor>/<filename>")
    def vendor_marc_record(self, vendor, filename):
        file_bytes = pathlib.Path(
            f"{DATA_EXPORT_FILES}/{vendor}/marc-files/{filename}"
        ).read_bytes()  # noqa
        return file_bytes
