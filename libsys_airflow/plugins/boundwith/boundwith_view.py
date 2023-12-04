from typing import Union

import pandas as pd

from airflow.models import DagBag

from flask import flash, request

from airflow.utils import timezone
from airflow.utils.state import State

from flask_appbuilder import expose, BaseView as AppBuilderBaseView


def trigger_bw_dag(
    bw_df: pd.DataFrame, sunid: str, user_email: Union[str, None]
) -> tuple:
    dagbag = DagBag("/opt/airflow/dags")
    dag = dagbag.get_dag("add_bw_relationships")
    execution_date = timezone.utcnow()
    run_id = f"manual__{execution_date.isoformat()}"
    dag.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        state=State.RUNNING,
        conf={
            "relationships": bw_df.to_dict(orient='records'),
            "email": user_email,
            "sunid": sunid,
        },
        external_trigger=True,
    )
    return run_id, execution_date


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
            raw_csv = request.files.get("upload-boundwith")
            email_addr = request.form.get("user-email")

            try:
                bw_df = pd.read_csv(raw_csv)
                if len(bw_df) > 1_000:
                    flash("Warning! CSV file has {len(bw_df)} rows, limit is 1,000")
                    rendered_page = self.render_template("boundwith/index.html")
                else:
                    run_id, execution_date = trigger_bw_dag(bw_df, sunid, email_addr)
                    rendered_page = self.render_template(
                        "boundwith/index.html",
                        run_id=run_id,
                        execution_date=execution_date,
                        user_email=email_addr,
                    )
            except pd.errors.EmptyDataError:
                flash("Warning! Empty CSV file for Boundwith Relationship DAG")
                rendered_page = self.render_template("boundwith/index.html")
        return rendered_page

    @expose("/")
    def bw_home(self):
        return self.render_template("boundwith/index.html")
