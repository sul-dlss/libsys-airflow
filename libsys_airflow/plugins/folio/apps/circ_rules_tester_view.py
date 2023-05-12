import datetime
import json
import pathlib

import pandas as pd

from airflow.models import DagBag
from airflow.utils import timezone
from airflow.utils.state import State

from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from flask import flash, request, redirect, Response

CIRC_HOME = "/opt/airflow/circ"


class CircRulesTester(AppBuilderBaseView):
    default_view = "circ_home"
    route_base = "/circ_rule_tester"

    def _trigger_dag_run(self, scenerio_file):
        dagbag = DagBag("/opt/airflow/dags")
        dag = dagbag.get_dag("circ_rules_batch_tests")
        scenerio_df = pd.read_csv(scenerio_file)
        execution_date = timezone.utcnow()
        run_id = f"manual__{execution_date.isoformat()}"
        dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf={"scenarios": scenerio_df.to_json()},
            external_trigger=True,
        )
        return run_id

    @expose("/")
    def circ_home(self):
        return self.render_template("circ_rules_tester/index.html")

    @expose("/batch_test", methods=["POST"])
    def run_batch_test(self):
        if "upload-scenarios" not in request.files:
            flash("No scenario file uploaded")
            return self.render_template("circ_rules_tester/index.html")
        scenario_file = request.files.get("upload-scenarios", "")
        if len(scenario_file.filename) < 1:
            flash("No selected scenario file")
        if not scenario_file.filename.endswith("csv"):
            flash("Scenario file must be a csv")
        else:
            dag_run_id = self._trigger_dag_run(scenario_file)
            return redirect(f"{CircRulesTester.route_base}/batch_report/{dag_run_id}")
        return self.render_template("circ_rules_tester/index.html")

    @expose("/test", methods=["POST"])
    def run_test(self):
        execution_date = timezone.utcnow()
        dagbag = DagBag("/opt/airflow/dags")
        dag = dagbag.get_dag("circ_rules_scenario_tests")
        run_id = f"manual__{execution_date.isoformat()}"
        dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf=dict(
                patron_group_id=request.form["patron_group_id"],
                material_type_id=request.form["material_type_id"],
                loan_type_id=request.form["loan_type_id"],
                location_id=request.form["location_id"],
            ),
            external_trigger=True,
        )
        return redirect(f"{CircRulesTester.route_base}/report/{run_id}")

    @expose("/batch_report/<dag_run>")
    def report_batch(self, dag_run):
        batch_report_path = pathlib.Path(f"{CIRC_HOME}/{dag_run}.json")
        if not batch_report_path.exists():
            flash(f"Report for DAG Run not completed. DAG ID {dag_run}")
            report = None
        else:
            report = pd.read_json(batch_report_path, encoding="utf-8-sig")
        return self.render_template(
            "circ_rules_tester/batch_report.html", dag_run=dag_run, report=report
        )

    @expose("/download/<dag_run>")
    def download_report(self, dag_run):
        batch_report_path = pathlib.Path(f"{CIRC_HOME}/{dag_run}.json")
        if not batch_report_path.exists():
            flash(f"Batch report DAG ID {dag_run} doesn't exist")
            return redirect(f"{CircRulesTester.route_base}")
        report = pd.read_json(batch_report_path, encoding="utf-8-sig")
        timestamp = datetime.datetime.utcnow()
        return Response(
            report.to_csv(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f"attachment;filename=batch_report_{timestamp.toordinal()}.csv"
            },
        )

    @expose("/report/<dag_run>")
    def report_scenario(self, dag_run):
        scenario_report_path = pathlib.Path(f"{CIRC_HOME}/{dag_run}.json")
        if not scenario_report_path.exists():
            flash(f"Report for DAG Run not completed. DAG ID {dag_run}")
            report = None
        else:
            with scenario_report_path.open(encoding="utf-8-sig") as report_fo:
                report = json.load(report_fo)
        return self.render_template(
            "circ_rules_tester/report.html", dag_run=dag_run, report=report
        )
