import pathlib

import pandas as pd

from airflow.models import DagBag
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import timezone
from airflow.utils.state import State

from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from flask import flash, request, redirect

CIRC_HOME = "/opt/airflow/circ"

class CircRulesTester(AppBuilderBaseView):
    default_view = "circ_home"
    route_base = "/circ_rule_tester"

    def _trigger_dag_runs(self, scenerio_file):
        dagbag = DagBag("/opt/airflow/dags")
        dag = dagbag.get_dag("single_circ_rules_tests")
        self.report_dagruns = []
        scenerio_df = pd.read_csv(scenerio_file)
        for row in scenerio_df.iterrows():
            execution_date = timezone.utcnow()
            run_id = f"manual__{execution_date.isoformat()}"
            conf = row[1].to_dict()
            conf.pop("status_name")
            dag.create_dagrun(
                run_id=run_id,
                execution_date=execution_date,
                state=State.RUNNING,
                conf=conf,
                external_trigger=True
            )
            self.report_dagruns.append(run_id)


    @expose("/")
    def circ_home(self):
        return self.render_template("circ_rules_tester/index.html")

    @expose("/batch_test", methods=["POST"])
    def run_batch_test(self):
        if 'upload-scenarios' not in request.files:
            flash("No scenario file uploaded")
            return self.render_template("circ_rules_tester/index.html")
        scenario_file = request.files.get('upload-scenarios', "")
        if len(scenario_file.filename) < 1:
            flash("No selected scenario file")
        if not scenario_file.filename.endswith("csv"):
            flash("Scenario file must be a csv")
        else:
            self._trigger_dag_runs(scenario_file)
            flash(f"Total dags {self.report_dagruns}")
        return self.render_template("circ_rules_tester/index.html")

    @expose("/test", methods=["POST"])
    def run_test(self):
        execution_date = timezone.utcnow()
        dagbag = DagBag("/opt/airflow/dags")
        dag = dagbag.get_dag("single_circ_rules_tests")
        run_id = f"manual__{execution_date.isoformat()}"
        dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf= dict(
                    patron_type_id = request.form["patron_type_id"],
                    item_type_id = request.form["item_type_id"],
                    loan_type_id = request.form["loan_type_id"],
                    location_id = request.form["location_id"]
            ),
            external_trigger=True
        )
        redirect(f"{CircRulesTester.route_base}/report?dag_run={run_id}")

    @expose("/report")
    def report_scenario(self, dag_run):
        scenario_report_path = pathlib.Path(f"{CIRC_HOME}/{dag_run}.json")
        if not scenario_report_path.exists():
            flash(f"Report for DAG Run not completed. DAG ID {dag_run}")
            report = None
        else:
            with scenario_report_path.open() as report_fo:
                report = json.load(report_fo)
        return self.render_template("circ_rules_tester/report.html", report=report)


