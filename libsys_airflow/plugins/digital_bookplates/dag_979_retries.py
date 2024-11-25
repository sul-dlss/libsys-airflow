import logging

from datetime import (
    datetime,
    timedelta,
    timezone,
)
from airflow.decorators import task
from airflow.models import DagRun
from airflow.models import Variable
from airflow.utils.state import DagRunState

from libsys_airflow.plugins.digital_bookplates.bookplates import (
    launch_digital_bookplate_979_dag,
    launch_poll_for_979_dags_email,
)

logger = logging.getLogger(__name__)


@task
def failed_979_dags() -> dict:
    """
    Find all of the failed digital_bookplate_979 DAG runs
    """
    start_date = datetime.now(timezone.utc) - timedelta(30)
    dag_runs = DagRun.find(
        state=DagRunState.FAILED,
        dag_id="digital_bookplate_979",
        execution_start_date=start_date,
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
    params = kwargs.get("dags", {})
    dag_runs = params.get("digital_bookplate_979s", {})
    devs_email_addr = Variable.get("EMAIL_DEVS")

    for dag in dag_runs:
        logger.info(f"Re-running dag with id: {dag}")
        dag_run = DagRun.find(run_id=dag)
        ti = dag_run[0].get_task_instance("retrieve_druids_for_instance_task")
        prev_val = ti.xcom_pull("retrieve_druids_for_instance_task")

        for key, value in prev_val.items():
            instance_id = key
            fund = value

            new_dag_run_id = launch_digital_bookplate_979_dag(
                instance_uuid=instance_id, funds=fund
            )
            logger.info(f"Launching new dag: {new_dag_run_id}")

    launch_poll_for_979_dags_email(dag_runs=dag_runs, email=devs_email_addr)

    return None
