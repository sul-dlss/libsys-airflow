import pandas as pd

from flask import flash, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from libsys_airflow.plugins.data_exports.instance_ids import save_ids

vendors = ['gobi', 'google', 'hathi', 'nielsen', 'oclc', 'pod', 'sharevde', 'west']


def upload_data_export_ids(ids_df: pd.DataFrame, vendor: str) -> str:
    tuples = list(ids_df.itertuples(index=False, name=None))
    ids_path = save_ids(airflow="/opt/airflow", vendor=vendor, data=tuples)

    return ids_path


def default_rendered_page(self):
    return self.render_template("data-export-upload/index.html", vendors=vendors)


class DataExportUploadView(AppBuilderBaseView):
    default_view = "data_export_upload_home"
    route_base = "/data_export_upload"

    @expose("/create", methods=["POST"])
    def run_data_export_upload(self):
        if "upload-data-export-ids" not in request.files:
            flash("Missing Instance UUID File")
        else:
            try:
                raw_csv = request.files["upload-data-export-ids"]
                vendor = request.form.get("vendor")
                ids_df = pd.read_csv(raw_csv)
                if len(ids_df) > 1_000:
                    flash(f"Warning! CSV file has {len(ids_df)} rows, limit is 1,000")
                else:
                    upload_data_export_ids(ids_df, vendor)
            except pd.errors.EmptyDataError:
                flash("Warning! Empty UUID file")
            except Exception as e:
                flash(f"Error with CSV: {e}")
            return default_rendered_page(self)

    @expose("/")
    def data_export_upload_home(self):
        return default_rendered_page(self)
