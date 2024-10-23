import pathlib
from flask import send_file
from flask_appbuilder import expose, BaseView as AppBuilderBaseView


class DigitalBookplatesDownloadView(AppBuilderBaseView):
    default_view = "digital_bookplates_download_home"
    route_base = "/digital_bookplates_download"
    files_base = "digital-bookplates"

    @expose("/")
    def digital_bookplates_download_home(self):
        content = []
        for path in pathlib.Path(f"{DigitalBookplatesDownloadView.files_base}").rglob(
            "*.csv"
        ):
            content.append(
                {
                    "date": f"{path.parent.parent.parent.name}-{path.parent.parent.name}-{path.parent.name}",
                    "year": path.parent.parent.parent.name,
                    "month": path.parent.parent.name,
                    "day": path.parent.name,
                    "filename": path.name,
                }
            )

        return self.render_template(
            "digital_bookplates_download/index.html", content=content
        )

    @expose("/<year>/<month>/<day>/<filename>")
    def csv_file(self, year, month, day, filename):
        folder_file = f"{year}-{month}-{day}-{filename}"
        return send_file(
            f"/opt/airflow/{self.files_base}/{year}/{month}/{day}/{filename}",
            as_attachment=True,
            mimetype="application/csv",
            download_name=folder_file,
        )
