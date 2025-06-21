"""Load MARC Authority and Bibiliographic Records into FOLIO."""

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context

from libsys_airflow.plugins.authority_control import (
    email_report,
    run_folio_data_import,
)

logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=datetime(2025, 2, 7),
    catchup=False,
    tags=["authorities", "folio"],
)
def load_marc_file(**kwargs):
    """DAG loads an Authority or Bib File into FOLIO."""

    @task
    def prepare_file_upload(*args, **kwargs):
        task_instance = kwargs["ti"]
        context = get_current_context()
        params = context.get("params", {})
        file_path = params["kwargs"].get("file")
        if file_path is None:
            raise ValueError("File path is required")
        task_instance.xcom_push(key="file_path", value=file_path)
        profile_name = params["kwargs"].get("profile")
        if profile_name is None:
            raise ValueError("Profile name is required")
        task_instance.xcom_push(key="profile_name", value=profile_name)
        return {"file_path": file_path, "profile_name": profile_name}

    @task
    def initiate_folio_data_import(file_path, profile_name):
        context = get_current_context()
        bash_operator = run_folio_data_import(file_path, profile_name)
        return bash_operator.execute(context)

    @task
    def email_load_report(**kwargs):
        return email_report(**kwargs)

    finished = EmptyOperator(task_id="finished-loading")

    dag_params = prepare_file_upload()
    bash_result = initiate_folio_data_import(
        dag_params["file_path"], dag_params["profile_name"]
    )

    email_load_report(bash_result=bash_result) >> finished


load_marc_file()
