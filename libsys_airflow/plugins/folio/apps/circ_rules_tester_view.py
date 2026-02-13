import datetime
import json
import pathlib

import pandas as pd

from airflow.models import DagBag
from airflow.utils import timezone
from airflow.utils.state import State

from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from flask import flash, request, redirect, Response

from libsys_airflow.plugins.shared.folio_client import folio_client

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
        timestamp = datetime.datetime.now(datetime.UTC)
        return Response(
            report.to_csv(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f"attachment;filename=batch_report_{timestamp.toordinal()}.csv"
            },
        )

    @expose("/reference")
    @expose("/reference/<data_type>")
    def reference_data(self, data_type=None):
        _folio_client = folio_client()
        is_download = bool(request.args.get('download'))
        match data_type:

            case "loan_type":
                title = "Loan Types"
                loan_types_df = pd.DataFrame(
                    _folio_client.folio_get(
                        "loan-types", key="loantypes", query_params={"limit": 999}
                    )
                )
                reference_df = loan_types_df.drop(columns=["metadata"]).rename(
                    columns={"name": "FOLIO name", "id": "UUID"}
                )

            case "locations":
                title = "Locations"
                locations_df = pd.DataFrame(_folio_client.locations)
                reference_df = locations_df.drop(
                    columns=[
                        'discoveryDisplayName',
                        'isActive',
                        'institutionId',
                        'campusId',
                        'libraryId',
                        'details',
                        'primaryServicePoint',
                        'servicePointIds',
                        'servicePoints',
                        'isShadow',
                        'metadata',
                        'description',
                    ]
                ).rename(
                    columns={"code": "FOLIO code", "name": "FOLIO name", "id": "UUID"}
                )

            case "material_type":
                title = "Material Types"
                material_types_df = pd.DataFrame(
                    _folio_client.folio_get(
                        "material-types", key="mtypes", query_params={"limit": 999}
                    )
                )
                reference_df = material_types_df.drop(
                    columns=["source", "metadata"]
                ).rename(columns={"name": "FOLIO name", "id": "UUID"})

            case "patron_group":
                title = "Patron Groups"
                patron_group_df = pd.DataFrame(
                    _folio_client.folio_get(
                        "/groups", key="usergroups", query_params={"limit": 999}
                    )
                )
                reference_df = patron_group_df.drop(
                    columns=["metadata", "expirationOffsetInDays"]
                ).rename(
                    columns={"group": "FOLIO code", "desc": "FOLIO name", "id": "UUID"}
                )

            case _:
                title = "Reference Data"
                reference_df = pd.DataFrame()

        if is_download and not reference_df.empty:
            return Response(
                reference_df.to_csv(index=False),
                mimetype="text/csv",
                headers={"Content-Disposition": f"attachment;filename={data_type}.csv"},
            )

        return self.render_template(
            "circ_rules_tester/reference-data.html",
            title=title,
            data_type=data_type,
            reference_df=reference_df,
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
