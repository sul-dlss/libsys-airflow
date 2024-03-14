import json
import pathlib
from flask_appbuilder import expose, BaseView as AppBuilderBaseView


parent = pathlib.Path(__file__).resolve().parent
vendor_file = open(parent / "vendors.json")
vendors = json.load(vendor_file)


def path_parent(full_path: pathlib.Path) -> str:
    parts = full_path.parts

    return parts[len(parts) - 2]


class DataExportDownloadView(AppBuilderBaseView):
    default_view = "data_export_download_home"
    route_base = "/data_export_download"

    @expose("/")
    def data_export_download_home(self):
        content = []
        for vendor in vendors['vendors']:
            for path in pathlib.Path(f"data-export-files/{vendor}/marc-files").glob(
                "*"
            ):
                content.append({vendor: [path_parent(path), path.name]})

            for path in pathlib.Path(f"data-export-files/{vendor}/transmitted").glob(
                "*"
            ):
                content.append({vendor: [path_parent(path), path.name]})

        return self.render_template("data-export-download/index.html", content=content)

    @expose("/downloads/<vendor>/<folder>/<filename>")
    def vendor_marc_record(self, vendor, folder, filename):
        file_bytes = pathlib.Path(
            f"data-export-files/{vendor}/{folder}/{filename}"
        ).read_bytes()  # noqa
        return file_bytes
