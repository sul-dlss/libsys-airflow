import pathlib

from flask_appbuilder import expose, BaseView as AppBuilderBaseView


LOOKUP_LIBRARY_CODE = {
    "CASUM": "Lane Medical Library",
    "HIN": "Hoover Institution Library and Archives",
    "RCJ": "Robert Crown Law Library",
    "S7Z": "Graduate School of Business",
    "STF": "Stanford University Libraries",
}

LOOKUP_REPORT_NAME = {
    "match": "Match BIB Record Errors",
    "multiple_oclc_numbers": "Multiple OCLC Numbers",
    "set_holdings": "Set OCLC Holdings Errors",
}


class DataExportOCLCReportsView(AppBuilderBaseView):
    default_view = "data_export_oclc_reports_home"
    route_base = "/data_export_oclc_reports"
    # files_base = "data-export-files"

    @expose("/")
    def data_export_oclc_reports_home(self):
        oclc_reports_home = pathlib.Path("/opt/airflow/data-export-files/oclc/reports")
        libraries = {}
        for library in oclc_reports_home.iterdir():
            if not library.is_dir():
                continue
            libraries[library.name] = {
                "name": LOOKUP_LIBRARY_CODE[library.name],
            }

            for report_type in library.iterdir():
                if not report_type.is_dir():
                    continue
                libraries[library.name][report_type.name] = {
                    "name": LOOKUP_REPORT_NAME[report_type.name],
                    "reports": [],
                }
                for report in report_type.glob("*.html"):
                    libraries[library.name][report_type.name]["reports"].append(report)

        return self.render_template(
            "data-export-oclc-reports/index.html", libraries=libraries
        )

    @expose("/<library_code>/<report_type>/<report_name>")
    def oclc_report(self, library_code, report_type, report_name):
        report_path = pathlib.Path(
            f"/opt/airflow/data-export-files/oclc/reports/{library_code}/{report_type}/{report_name}"
        )

        return self.render_template(
            "data-export-oclc-reports/report.html",
            library_name=LOOKUP_LIBRARY_CODE[library_code],
            report_name=LOOKUP_REPORT_NAME[report_type],
            contents=report_path.read_text(),
        )
