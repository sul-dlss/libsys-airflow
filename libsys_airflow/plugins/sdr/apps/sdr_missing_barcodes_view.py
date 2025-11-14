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


class SdrMissingBarcodesView(AppBuilderBaseView):
    default_view = "sdr_missing_barcodes_home"
    route_base = "/sdr"
    reports_base = pathlib.Path("/opt/airflow/sdr-files/reports")

    @expose("/")
    def sdr_missing_barcodes_home(self):
        missing_barcode_files = [
            _file_info(row) for row in self.reports_base.glob("*.csv")
        ]
        return self.render_template(
            "sdr/index.html", missing_barcodes_files=missing_barcode_files
        )

    @expose("/<file_name>")
    def download(self, file_name):
        report_path = self.reports_base / file_name
        return send_file(
            str(report_path),
            as_attachment=True,
            mimetype="application/csv",
            download_name=file_name,
        )
