from typing import Union

import pandas as pd

from flask import flash, request

from airflow_client.client import DagRunApi, TriggerDAGRunPostBody
from libsys_airflow.plugins.shared.airflow_api_client import api_client

from flask_appbuilder import expose, BaseView as AppBuilderBaseView


def trigger_bw_dag(
    bw_df: pd.DataFrame, sunid: str, user_email: Union[str, None], file_name: str
) -> str:
    dag_id = "add_bw_relationships"
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        trigger_dag_run_post_body = TriggerDAGRunPostBody(
            conf={
                "relationships": bw_df.to_dict(orient='records'),
                "email": user_email,
                "sunid": sunid,
                "file_name": file_name,
            }
        )

        api_response = api_instance.trigger_dag_run(dag_id, trigger_dag_run_post_body)

    return api_response.dag_run_id


class BoundWithView(AppBuilderBaseView):
    default_view = "bw_home"
    route_base = "/boundwith"

    @expose("/create", methods=["POST"])
    def run_bw_creation(self):
        sunid = request.form.get("sunid")
        if "upload-boundwith" not in request.files:
            flash("Missing Boundwith Relationship File")
            rendered_page = self.render_template("boundwith/index.html")
        elif sunid is None or len(sunid.strip()) < 1:
            flash("SUNID Required")
            rendered_page = self.render_template("boundwith/index.html")
        else:
            try:
                raw_csv = request.files["upload-boundwith"]
                email_addr = request.form.get("user-email")
                bw_df = pd.read_csv(raw_csv)
                if ["part_holdings_hrid", "principle_barcode"] != list(bw_df.columns):
                    flash(f"Invalid columns: {list(bw_df.columns)} for CSV file")
                    rendered_page = self.render_template("boundwith/index.html")
                elif len(bw_df) < 2:
                    flash(f"Warning! CSV file only contains one row. Need to include row for the principle's barcode and holdings HRID.")
                    rendered_page = self.render_template("boundwith/index.html")
                elif len(bw_df) > 1_000:
                    flash(f"Warning! CSV file has {len(bw_df)} rows, limit is 1,000")
                    rendered_page = self.render_template("boundwith/index.html")
                else:
                    run_id = trigger_bw_dag(bw_df, sunid, email_addr, raw_csv.filename)
                    rendered_page = self.render_template(
                        "boundwith/index.html",
                        run_id=run_id,
                        user_email=email_addr,
                    )
            except pd.errors.EmptyDataError:
                flash("Warning! Empty CSV file for Boundwith Relationship DAG")
                rendered_page = self.render_template("boundwith/index.html")
            except Exception as e:
                flash(f"Error with CSV {e}")
                rendered_page = self.render_template("boundwith/index.html")
        return rendered_page

    @expose("/")
    def bw_home(self):
        return self.render_template("boundwith/index.html")
