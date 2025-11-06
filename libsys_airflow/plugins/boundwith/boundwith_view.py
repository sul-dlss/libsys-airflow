from typing import Union

import pandas as pd

from flask import flash, request

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from libsys_airflow.plugins.shared.utils import execution_date

from flask_appbuilder import expose, BaseView as AppBuilderBaseView


def trigger_bw_dag(
    bw_df: pd.DataFrame, sunid: str, user_email: Union[str, None], file_name: str
) -> tuple:
    logical_date = execution_date()
    run_id = f"manual__{logical_date}"
    TriggerDagRunOperator(
        task_id="trigger_bw_dag",
        trigger_dag_id="add_bw_relationships",
        trigger_run_id=run_id,
        logical_date=logical_date,
        conf={
            "relationships": bw_df.to_dict(orient='records'),
            "email": user_email,
            "sunid": sunid,
            "file_name": file_name,
        }
    )
    return run_id, logical_date


class BoundWithView(AppBuilderBaseView):
    default_view = "bw_home"
    route_base = "/boundwith"

    @expose("/create", methods=["POST"])
    def run_bw_creation(self):
        sunid = request.form.get("sunid")
        if "upload-boundwith" not in request.files:
            flash("Missing Boundwith Relationship File")
            rendered_page = self.render_template("boundwith/index.html")
        elif len(sunid.strip()) < 1:
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
                elif len(bw_df) > 1_000:
                    flash(f"Warning! CSV file has {len(bw_df)} rows, limit is 1,000")
                    rendered_page = self.render_template("boundwith/index.html")
                else:
                    run_id, execution_date = trigger_bw_dag(
                        bw_df, sunid, email_addr, raw_csv.filename
                    )
                    rendered_page = self.render_template(
                        "boundwith/index.html",
                        run_id=run_id,
                        execution_date=execution_date,
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
