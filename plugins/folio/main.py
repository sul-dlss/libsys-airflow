import csv
import pathlib

import markdown

from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

MIGRATION_HOME = "/opt/airflow/migration"

bp = Blueprint(
    "folio_plugin",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/folio_plugin",
)


def _extract_dag_run_id(file_path: pathlib.Path) -> str:
    """Extracts DAG ID run from file name, varies depending on file type"""
    dag_run_id = None
    path_parts = file_path.name.split("_")
    if path_parts[0].startswith("transformation"):
        dag_run_id = "_".join(path_parts[3:6])
    if path_parts[0:3] == ["failed", "bib", "records"] and file_path.suffix == ".mrc":  # noqa
        dag_run_id = "_".join(path_parts[3:6])
        # remove file extension
        dag_run_id = dag_run_id.replace(".mrc", "")
    return dag_run_id


class FOLIO(AppBuilderBaseView):
    default_view = "folio_view"

    @expose("/")
    def folio_view(self):
        content = {}
        for path in pathlib.Path(f"{MIGRATION_HOME}/reports").iterdir():
            dag_run_id = _extract_dag_run_id(path)
            if dag_run_id not in content:
                content[dag_run_id] = {
                    "reports": [],
                    "data_issues": [],
                    "marc_errors": [],
                }
            if path.name.startswith("transformation"):
                content[dag_run_id]["reports"].append(path.name)
            if path.name.startswith("data_issues"):
                content[dag_run_id]["data_issues"].append(path.name)
        
        for path in pathlib.Path(f"{MIGRATION_HOME}/results").glob("failed_*.mrc"):  # noqa
            dag_run_id = _extract_dag_run_id(path)
            if dag_run_id is None:
                continue
            if dag_run_id not in content:
                print(content.keys())
                continue
            content[dag_run_id]["marc_errors"].append(
                {"file": path.name, "size": path.stat().st_size}
            )
        return self.render_template("folio/index.html", content=content)

    @expose("/reports/<report_name>")
    def folio_report(self, report_name):
        markdown_path = pathlib.Path(f"{MIGRATION_HOME}/reports/{report_name}")
        raw_text = markdown_path.read_text()
        # Sets attribute to convert markdown in embedded HTML tags
        final_mrkdown = raw_text.replace("<details>",
                                         """<details markdown="block">""")
        rendered = markdown.markdown(final_mrkdown,
                                     extensions=["tables", "md_in_html"])
        return self.render_template("folio/report.html", content=rendered)

    @expose("/data_issues/<log_name>")
    def folio_data_issues(self, log_name):
        tsv_path = pathlib.Path(f"{MIGRATION_HOME}/results/{log_name}")
        tsv_reader = csv.reader(tsv_path, delimiter="\t")
        tsv_rows = [r for r in tsv_reader]
        return self.render_template("folio/data-issues.html", content=tsv_rows)

    @expose("/marc/<filename>")
    def folio_marc_error(self, filename):
        file_bytes = pathlib.Path(f"{MIGRATION_HOME}/results/{filename}").read_bytes()  # noqa
        return file_bytes


v_appbuilder_view = FOLIO()
v_appbuilder_package = {
    "name": "FOLIO Reports and Logs",
    "category": "FOLIO",
    "view": v_appbuilder_view,
}


class FOLIOPlugin(AirflowPlugin):
    name = "FOLIOInformation"
    operators = []
    flask_blueprints = [bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [v_appbuilder_package]
    appbuilder_menu_items = []
