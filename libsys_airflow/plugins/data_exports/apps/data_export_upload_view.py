import json
import pandas as pd
import pathlib
import re

from flask import flash, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from libsys_airflow.plugins.data_exports.instance_ids import save_ids


parent = pathlib.Path(__file__).resolve().parent
vendor_file = open(parent / "vendors.json")
vendors = json.load(vendor_file)


def upload_data_export_ids(ids_df: pd.DataFrame, vendor: str) -> str:
    if len(ids_df.columns) > 1:
        raise ValueError("ID file has more than one column.")
    tuples = list(ids_df.itertuples(index=False, name=None))
    for id in tuples:
        if not re.search(
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', id[0]
        ):
            raise ValueError(f"{id[0]} is not a UUID.")

    ids_path = save_ids(airflow="/opt/airflow", vendor=vendor, data=tuples)

    return ids_path


def default_rendered_page(self):
    return self.render_template(
        "data-export-upload/index.html", vendors=vendors['vendors']
    )


class DataExportUploadView(AppBuilderBaseView):
    default_view = "data_export_upload_home"
    route_base = "/data_export_upload"

    @expose("/create", methods=["POST"])
    def run_data_export_upload(self):
        if "upload-data-export-ids" not in request.files:
            flash("Missing Instance UUID File.")
        else:
            try:
                raw_csv = request.files["upload-data-export-ids"]
                vendor = request.form.get("vendor")
                ids_df = pd.read_csv(raw_csv)
                if not vendor:
                    raise Exception("You must choose a vendor!")
                else:
                    upload_data_export_ids(ids_df, vendor)
                    flash("Sucessfully uploaded ID file.")
            except pd.errors.EmptyDataError:
                flash("Warning! Empty UUID file.")
            except Exception as e:
                flash(f"Error: {e}")
            return default_rendered_page(self)

    @expose("/")
    def data_export_upload_home(self):
        return default_rendered_page(self)
