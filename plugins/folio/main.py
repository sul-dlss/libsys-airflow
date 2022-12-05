import json
import pathlib

import markdown
import pandas as pd


from airflow.plugins_manager import AirflowPlugin


from flask import Blueprint, flash, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from plugins.folio.apps.circ_rules_tester import CircRulesTester as Circ_Rules_Tester

CIRC_HOME = "/opt/airflow/circ"
MIGRATION_HOME = "/opt/airflow/migration"

bp = Blueprint(
    "folio_plugin",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/folio_plugin",
)


def _get_catkey_range(path):
    start, end = None, None

    try:
        marc_path = next((path / "source_data/instances").glob("*.mrc"))
    except StopIteration:
        # File doesn't exist
        return start, end

    name_parts = marc_path.name.split("_")
    if len(name_parts) < 3:  # Non-standard name
        return start, end

    start_part, end_part = name_parts[1], name_parts[2].split(".")[0]
    try:
        start = f"{int(start_part):,}"
    except ValueError:
        pass
    try:
        end = f"{int(end_part):,}"
    except ValueError:
        pass
    return start, end


def _get_folio_records(path):
    records = {
        "instances": [],
        "holdings": [],
        "items": [],
        "srs": []
    }
    for result_path in (path / "results").glob("folio_*"):
        result_name = result_path.name
        match result_name:
            case result_name if result_name.startswith("folio_instances"):
                records["instances"].append(result_name)

            case result_name if result_name.startswith("folio_holdings"):
                records["holdings"].append(result_name)

            case result_name if result_name.startswith("folio_items"):
                records["items"].append(result_name)

            case result_name if result_name.startswith("folio_srs"):
                records["srs"].append(result_name)

    return records


def _get_reports_data_issues(path):
    reports, data_issues = [], []
    for report_path in (path / "reports").iterdir():
        report_name = report_path.name
        if report_name.startswith("data_issues"):
            data_issues.append(report_name)
        if report_name.startswith("report"):
            reports.append(report_name)
    return reports, data_issues


def _get_source_data(path):
    sources = {"instances": [], "holdings": [], "items": []}
    for instance_mrc in (path / "source_data/instances").glob("*.mrc"):
        sources["instances"].append(instance_mrc.name)
    for mhlds_mrc in (path / "source_data/holdings").glob("*.mrc"):
        sources["holdings"].append(mhlds_mrc.name)
    for file_path in (path / "source_data/items").glob("*.tsv"):
        file_name = file_path.name
        sources["holdings"].append(file_name)
        sources["items"].append(file_name)
    return sources


class FOLIO(AppBuilderBaseView):
    default_view = "folio_view"

    @expose("/")
    def folio_view(self):
        content = {}
        for path in pathlib.Path(f"{MIGRATION_HOME}/iterations").glob("manual__*"):
            dag_run_id = path.name
            if dag_run_id not in content:
                content[dag_run_id] = {}
            reports, data_issues = _get_reports_data_issues(path)
            content[dag_run_id]["reports"] = reports
            content[dag_run_id]["data_issues"] = data_issues
            content[dag_run_id]["records"] = _get_folio_records(path)
            content[dag_run_id]["sources"] = _get_source_data(path)
            (
                content[dag_run_id]["ckey_start"],
                content[dag_run_id]["ckey_end"],
            ) = _get_catkey_range(path)

        return self.render_template("folio/index.html", content=content)

    @expose("/reports/<iteration_id>/<report_name>")
    def folio_report(self, iteration_id, report_name):
        markdown_path = pathlib.Path(
            f"{MIGRATION_HOME}/iterations/{iteration_id}/reports/{report_name}"
        )
        raw_text = markdown_path.read_text()
        # Sets attribute to convert markdown in embedded HTML tags
        final_mrkdown = raw_text.replace("<details>", """<details markdown="block">""")
        rendered = markdown.markdown(final_mrkdown, extensions=["tables", "md_in_html"])
        return self.render_template("folio/report.html", content=rendered)

    @expose("/data_issues/<iteration_id>/<log_name>")
    def folio_data_issues(self, iteration_id, log_name):
        data_issues_df = pd.read_csv(
            f"{MIGRATION_HOME}/iterations/{iteration_id}/reports/{log_name}",
            sep="\t",
            names=["Type", "Catkey", "Error", "Value"],
        )

        return self.render_template(
            "folio/data-issues.html",
            content={"df": data_issues_df, "dag_run_id": iteration_id},
        )

    @expose("/records/<iteration_id>/<filename>")
    def folio_json_records(self, iteration_id, filename):
        file_bytes = pathlib.Path(
            f"{MIGRATION_HOME}/iterations/{iteration_id}/results/{filename}"
        ).read_bytes()  # noqa
        return file_bytes

    @expose("/sources/<iteration_id>/<folio_type>/<filename>")
    def source_record(self, iteration_id, folio_type, filename):
        file_bytes = pathlib.Path(
            f"{MIGRATION_HOME}/iterations/{iteration_id}/source_data/{folio_type}/{filename}"
        ).read_bytes()  # noqa
        return file_bytes


folio_appbuilder_view = FOLIO()
folio_appbuilder_package = {
    "name": "FOLIO Reports and Logs",
    "category": "FOLIO",
    "view": folio_appbuilder_view,
}

# Circ Rules Tester App
circ_rules_tester_view = Circ_Rules_Tester()
circ_rules_tester_package = {
    "name": "Circ Rules Tester",
    "category": "FOLIO",
    "view": circ_rules_tester_view
}


class FOLIOPlugin(AirflowPlugin):
    name = "FOLIOInformation"
    operators = []
    flask_blueprints = [bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [folio_appbuilder_package, circ_rules_tester_package]
    appbuilder_menu_items = []
