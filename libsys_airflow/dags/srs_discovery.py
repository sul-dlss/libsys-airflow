import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from libsys_airflow.plugins.folio.helpers.marc import discover_srs_files

logger = logging.getLogger(__name__)


with DAG(
    "srs_audit_discovery",
    default_args={
        "owner": "folio",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule=None,
    start_date=datetime(2023, 3, 2),
    catchup=False,
    tags=["bib_import", "folio"],
    max_active_runs=1,
) as dag:

    @task
    def discover_srs_iterations():
        return discover_srs_files()

    @task
    def launch_srs_remediation(**kwargs):
        iterations = kwargs.get('iterations', [])

        for i, iteration in enumerate(iterations):
            TriggerDagRunOperator(
                task_id=f"srs_audit_checks_{i}",
                trigger_dag_id="srs_audit_checks",
                conf={"iteration": iteration},
            ).execute(kwargs)

    iterations = discover_srs_iterations()

    launch_srs_remediation(iterations=iterations)
