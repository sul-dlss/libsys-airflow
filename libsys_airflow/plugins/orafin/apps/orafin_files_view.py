import datetime
import pathlib

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
    files_base = "/opt/airflow/orafin-files"

    @expose("/")
    def orafin_files_home(self):
        orafin_base = pathlib.Path(self.files_base)
        data = orafin_base / "data"
        reports = orafin_base / "reports"

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
