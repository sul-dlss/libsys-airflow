import datetime
import pathlib

from flask import send_file
from flask_appbuilder import expose, BaseView as AppBuilderBaseView


def _file_info(file: pathlib.Path) -> dict:
    stats = file.stat()
    created_date = datetime.datetime.fromtimestamp(stats.st_ctime)
    return {
        "name": file.name,
        "date_created": created_date.isoformat(),
        "size": f"{stats.st_size:,}",
    }


class OrafinFilesView(AppBuilderBaseView):
    default_view = "orafin_files_home"
    route_base = "/orafin"
    files_base = pathlib.Path("/opt/airflow/orafin-files")

    @expose("/")
    def orafin_files_home(self):
        data = self.files_base / "data"
        reports = self.files_base / "reports"

        feeder_files = []
        for feeder_file in data.iterdir():
            if feeder_file.is_file():
                feeder_files.append(_file_info(feeder_file))

        ap_reports = []
        for report in reports.iterdir():
            if report.is_file():
                ap_reports.append(_file_info(report))

        return self.render_template(
            "orafin/index.html", feeder_files=feeder_files, ap_reports=ap_reports
        )

    @expose("/<type_of>/<file_name>")
    def download(self, type_of, file_name):
        orafin_file_path = self.files_base / type_of / file_name
        mimetype = "application/csv"
        if type_of == "data":
            mimetype = "application/text"
        return send_file(
            str(orafin_file_path),
            as_attachment=True,
            mimetype=mimetype,
            download_name=file_name,
        )
