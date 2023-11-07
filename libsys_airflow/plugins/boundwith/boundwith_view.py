import pandas as pd

from airflow.models import DagBag

from flask import flash, request

from airflow.utils import timezone
from airflow.utils.state import State

from flask_appbuilder import expose, BaseView as AppBuilderBaseView


def trigger_bw_dag(bw_df: pd.DataFrame) -> str:
    dagbag = DagBag("/opt/airflow/dags")
    dag = dagbag.get_dag("add_bw_relationships")
    execution_date = timezone.utcnow()
    run_id = f"manual__{execution_date.isoformat()}"
    dag.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        state=State.RUNNING,
        conf={"relationships": bw_df.to_dict(orient='records')},
        external_trigger=True,
    )
    return run_id, execution_date


class BoundWithView(AppBuilderBaseView):
    default_view = "bw_home"
    route_base = "/boundwith"

    @expose("/create", methods=["POST"])
    def run_bw_creation(self):
        if "upload-boundwith" not in request.files:
            flash("Missing Boundwith Relationship File")
            rendered_page = self.render_template("boundwith/index.html")
        else:
            raw_csv = request.files.get("upload-boundwith")
            try:
                bw_df = pd.read_csv(raw_csv)
                run_id, execution_date = trigger_bw_dag(bw_df)
                rendered_page = self.render_template(
                    "boundwith/index.html", run_id=run_id, execution_date=execution_date
                )
            except pd.errors.EmptyDataError:
                flash("Warning! Empty CSV file for Boundwith Relationship DAG")
                rendered_page = self.render_template("boundwith/index.html")
        return rendered_page

    @expose("/")
    def bw_home(self):
        return self.render_template("boundwith/index.html")
