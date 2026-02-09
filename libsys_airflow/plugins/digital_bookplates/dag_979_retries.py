import logging

from airflow.sdk import task, Variable
from airflow.providers.standard.operators.bash import BashOperator

from libsys_airflow.plugins.digital_bookplates.bookplates import (
    launch_poll_for_979_dags_email,
)

logger = logging.getLogger(__name__)


def failed_979_dags():
    """
    Find all of the failed digital_bookplate_979 DAG runs
    """
    dag_runs = BashOperator(
        task_id="failed_dag_runs",
        bash_command="airflow dags list-runs digital_bookplate_979s --state=failed --output=json"
    )
    return dag_runs


@task
def run_ids(failed_dag_runs: list) -> list:
    """
    Gets run_ids from list of failed dag runs
    [{"dag_id": "course_reserves_data", "run_id": "scheduled__2025-11-06T00:40:00+00:00", "state": "failed", "run_after": "2025-11-06T09:40:00+00:00", "logical_date": "2025-11-06T00:40:00+00:00", "start_date": "2025-11-06T09:40:00.126471+00:00", "end_date": "2025-11-06T10:40:52.175757+00:00"},]
    """
    run_ids = []
    for _ in failed_dag_runs:
        run_id = _["run_id"]
        logger.info(f"Found: {run_id}")
        run_ids.append(run_id)

    return run_ids


@task.bash
def clear_failed_add_marc_tags_to_record() -> str:
    """
    Re-run the failed digital_bookplate_979 DAGs by clearing the add_marc_tags_to_record task
    airflow tasks clear digital_bookplate_979 -t add_marc_tags_to_record --only-failed --downstream --yes
    --yes = Do not prompt to confirm
    """
    logger.info("Clearing failed add_marc_tags_to_record tasks for digital_bookplate_979 DAG runs.")
    return "airflow tasks clear digital_bookplate_979 -t add_marc_tags_to_record --only-failed --downstream --yes"


@task
def poll_for_979s_dags(run_ids):
    devs_email_addr = Variable.get("EMAIL_DEVS")

    logger.info(f"{len(run_ids)} failed 979 DAG runs queued: {run_ids}")

    launch_poll_for_979_dags_email(dag_runs=run_ids, email=devs_email_addr)

    return None
