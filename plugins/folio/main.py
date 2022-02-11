import logging
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
    static_url_path="/static/folio_plugin"
)

class FOLIO(AppBuilderBaseView):
    default_view = "folio_view"

    @expose("/")
    def folio_view(self):
        content = { }
        for path in pathlib.Path(f"{MIGRATION_HOME}/reports").glob("*.md"):
            reports.append(path.name)
        return self.render_template("folio/index.html", content=reports)

    @expose("/<report_name>")
    def folio_report(self, report_name):
        markdown_path = pathlib.Path(f"/opt/airflow/migration/reports/{report_name}")
        rendered = markdown.markdown(markdown_path.read_text(), extensions=['md_in_html'])
        return self.render_template("folio/report.html", content=rendered)

    @expose("/<log_name>")
    def folio_data_issues(self, log_name):
        tsv_path = pathlib



v_appbuilder_view = FOLIO()
v_appbuilder_package = {
    "name": "FOLIO Reports and Logs",
    "category": "FOLIO",
    "view": v_appbuilder_view
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