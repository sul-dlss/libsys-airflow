import json
import pathlib
from flask import send_file
from flask_appbuilder import expose, BaseView as AppBuilderBaseView


parent = pathlib.Path(__file__).resolve().parent
vendor_file = open(parent / "vendors.json")
vendors = json.load(vendor_file)


class DataExportDownloadView(AppBuilderBaseView):
    default_view = "data_export_download_home"
    route_base = "/data_export_download"
    files_base = "data-export-files"

    @expose("/")
    def data_export_download_home(self):
        content = []
        for vendor in vendors['vendors']:
            for state in ["marc-files", "transmitted"]:
                for kind in ["new", "updates", "deletes"]:
                    for path in pathlib.Path(
                        f"{DataExportDownloadView.files_base}/{vendor}/{state}/{kind}"
                    ).glob("*"):
                        content.append({vendor: [state, kind, path.name]})

        return self.render_template("data-export-download/index.html", content=content)

    @expose("/downloads/<vendor>/<state>/<folder>/<filename>")
    def vendor_marc_record(self, vendor, state, folder, filename):
        folder_file = f"{vendor}-{state}-{folder}-{filename}"
        return send_file(
            f"/opt/airflow/data-export-files/{vendor}/{state}/{folder}/{filename}",
            as_attachment=True,
            mimetype="application/marc",
            download_name=folder_file,
        )
