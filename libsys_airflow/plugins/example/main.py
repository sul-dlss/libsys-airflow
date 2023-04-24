import logging
import requests
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from airflow.models.serialized_dag import SerializedDagModel
from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

logger = logging.getLogger(__name__)


bp = Blueprint(
    "example_plugin",
    __name__,
    template_folder="templates", # registers airflow/plugins/example/templates as a Jinja template folder
    static_folder="static",
    static_url_path="/static/example_plugin",
)

# https://docs.astronomer.io/learn/airflow-database#database-specifications
# this is a direct query to the metadata database: use at your own risk!
stmt = """SELECT version_num
        FROM alembic_version;"""

# retrieving SQL Alchemy connection
sql_alchemy_conn = os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"]
conn_url = f"{sql_alchemy_conn}"
engine = create_engine(conn_url)

with Session(engine) as session:
    result = session.query(SerializedDagModel).first()
    dag_dependencies = result.get_dag_dependencies()
    alembic_v = session.execute(stmt).all()[0][0]
    

class ExampleBaseView(AppBuilderBaseView):
    default_view = "home"
    route_base = "/example"

    @expose("/")
    def home(self):
        info = "pollen"
        return self.render_template("index.html", info=info)
    
    @expose("/dag-info")
    def dag_info(self):
        # API Query example
        ENDPOINT_URL = "http://localhost:8080"
        # would use env variables, e.g.:
        # user_name = os.environ["USERNAME_AIRFLOW_INSTANCE"]
        # password = os.environ["PASSWORD_AIRFLOW_INSTANCE"]
        user_name = "airflow"
        password = "airflow"

        # query the API for all dags
        req = requests.get(
            f"{ENDPOINT_URL}/api/v1/dags",
            auth=(user_name, password),
        )
        logger.info(req.json())

        entries = req.json()["total_entries"]
        dags_list = req.json()["dags"]

        return self.render_template("dags-list.html", dags_list=dags_list, entries=entries, dag_dependencies=dag_dependencies, alembic_v=alembic_v)


# instantiate ExampleBaseView
example_view = ExampleBaseView()

# define the path to example_view in the Airflow UI
example_package = {
    # define the menu sub-item name
    "name": "Example",
    # define the top-level menu item
    "category": "Test Example",
    "view": example_view
}


class ExamplePlugin(AirflowPlugin):
    name = "example_plugin"
    operators = []
    flask_blueprints = [bp]
    hooks = []
    executors = []
    admin_views = []
    appbuilder_views = [example_package]
    appbuilder_menu_items = []




    