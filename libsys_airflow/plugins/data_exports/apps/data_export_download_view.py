import json
import pathlib
from flask import send_file
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
            for path in pathlib.Path(f"data-export-files/{vendor}/marc-files/new").glob(
                "*"
            ):
                content.append({vendor: [path_parent(path), path.name]})

            for path in pathlib.Path(
                f"data-export-files/{vendor}/marc-files/updates"
            ).glob("*"):
                content.append({vendor: [path_parent(path), path.name]})

            for path in pathlib.Path(
                f"data-export-files/{vendor}/marc-files/deletes"
            ).glob("*"):
                content.append({vendor: [path_parent(path), path.name]})

            for path in pathlib.Path(f"data-export-files/{vendor}/transmitted").glob(
                "*"
            ):
                content.append({vendor: [path_parent(path), path.name]})

        return self.render_template("data-export-download/index.html", content=content)

    @expose("/downloads/<vendor>/<folder>/<filename>")
    def vendor_marc_record(self, vendor, folder, filename):
        folder_file = f"{folder}-{filename}"
        return send_file(
            f"/opt/airflow/data-export-files/{vendor}/marc-files/{folder}/{filename}",
            as_attachment=True,
            mimetype="application/marc",
            download_name=folder_file,
        )
