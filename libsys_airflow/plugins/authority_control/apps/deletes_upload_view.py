import datetime
import pathlib

import pandas as pd

from flask import flash, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from airflow_client.client import DagRunApi, TriggerDAGRunPostBody

from libsys_airflow.plugins.shared.airflow_api_client import api_client


class AuthorityRecordsDeleteUploadView(AppBuilderBaseView):

    default_view = "authorities_delete_home"
    route_base = "/authorities_deletes"

    def _save_deletes_csv(self, deletes_df: pd.DataFrame, filename: str) -> str:
        authority_uploads_path = pathlib.Path("/opt/airflow/authorities/uploads")
        authority_uploads_path.mkdir(parents=True, exist_ok=True)
        deletes_csv_path = authority_uploads_path / filename
        deletes_df.to_csv(deletes_csv_path, index=False)
        return str(deletes_csv_path.absolute())

    def _trigger_dag_run(self, deletes_csv_file: str, email: str | None = None) -> str:
        with api_client() as airflow_api_client:
            api_instance = DagRunApi(airflow_api_client)
            trigger_body = TriggerDAGRunPostBody(
                conf={"kwargs": {"file": deletes_csv_file, "email": email}}
            )
            api_response = api_instance.trigger_dag_run(
                "delete_authority_records", trigger_body
            )
            return api_response.dag_run_id

    @expose("/upload", methods=["POST"])
    def upload_csv(self):
        email = request.form.get("email")
        if "upload-deletes" not in request.files:
            flash("Missing file upload")
            rendered_template = self.render_template("deletes-csv-upload/index.html")
        try:
            raw_csv = request.files["upload-deletes"]
            deletes_csv_df = pd.read_csv(raw_csv, names=["001s"])
            deletes_csv_file = self._save_deletes_csv(deletes_csv_df, raw_csv.filename)
            run_id = self._trigger_dag_run(deletes_csv_file, email)
            rendered_template = self.render_template(
                "deletes-csv-upload/index.html", run_id=run_id, email=email
            )
        except pd.errors.EmptyDataError:
            flash("Upload csv file is empty")
            rendered_template = self.render_template("deletes-csv-upload/index.html")
        except Exception as e:
            flash(f"Error with upload: {e}")
            rendered_template = self.render_template("deletes-csv-upload/index.html")

        return rendered_template

    @expose("/")
    def authorities_delete_home(self):
        return self.render_template("deletes-csv-upload/index.html")
