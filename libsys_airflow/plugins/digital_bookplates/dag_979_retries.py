import logging

from datetime import datetime, timedelta, timezone

from airflow.decorators import task
from airflow.models import DagBag, DagRun, Variable
from airflow.utils.state import DagRunState

from libsys_airflow.plugins.digital_bookplates.bookplates import (
    launch_poll_for_979_dags_email,
)

logger = logging.getLogger(__name__)


@task
def failed_979_dags() -> dict:
    """
    Find all of the failed digital_bookplate_979 DAG runs
    """
    dag_runs = DagRun.find(
        state=DagRunState.FAILED,
        dag_id="digital_bookplate_979",
    )
    db_979_dags: dict = {"digital_bookplate_979s": []}
    for dag_run in dag_runs:
        logger.info(f"Found: {dag_run.run_id}")
        db_979_dags["digital_bookplate_979s"].append(dag_run.run_id)

    return db_979_dags


@task
def run_failed_979_dags(**kwargs):
    """
    Re-run the failed digital_bookplate_979 DAGs and launch the email poll
    """
    params = kwargs.get("dag_runs", {})
    devs_email_addr = Variable.get("EMAIL_DEVS")

    dag_runs = params.get("digital_bookplate_979s", {})
    logger.info(f"Clearing failed 979 DAG runs: {dag_runs}")

    dagbag = DagBag("/opt/airflow/dags")
    dag = dagbag.get_dag("digital_bookplate_979")
    dag.clear_dags(
        dags=[dag],
        only_failed=True,
        dry_run=False,
    )

    launch_poll_for_979_dags_email(dag_runs=dag_runs, email=devs_email_addr)

    return None
