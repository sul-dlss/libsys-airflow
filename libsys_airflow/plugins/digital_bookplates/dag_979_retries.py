import logging
import json
from airflow.sdk import task, Variable
from airflow_client.client import DagRunApi, DAGRunClearBody
from airflow_client.client.models import (
    DAGRunCollectionResponse,
    DAGRunResponse,
    ResponseClearDagRun,
)
from airflow_client.client.rest import ApiException

from libsys_airflow.plugins.digital_bookplates.bookplates import (
    launch_poll_for_979_dags_email,
)
from libsys_airflow.plugins.shared.airflow_api_client import (
    api_client,
)

logger = logging.getLogger(__name__)


def get_all_failed_dag_runs(
    api_instance: DagRunApi, size: int, initial_limit: int, initial_offset: int
) -> list[DAGRunResponse]:
    all_dag_runs: list = []
    limit = initial_limit
    offset = initial_offset
    while offset < size:
        try:
            api_response: DAGRunCollectionResponse = api_instance.get_dag_runs(
                dag_id="digital_bookplate_979",
                state=["failed"],
                limit=limit,
                offset=offset,
            )
            all_dag_runs.extend(api_response.dag_runs)
        except ApiException as e:
            logger.warning(f"Exception when calling DagRunApi: {e}")
            continue

        # Increment the limit and offset
        limit = min(limit + 100, size - offset)
        offset += limit

    return all_dag_runs


def total_failed_dag_runs(api_instance: DagRunApi) -> int | None:
    try:
        api_response: DAGRunCollectionResponse = api_instance.get_dag_runs(
            dag_id="digital_bookplate_979", state=["failed"]
        )
        total_entries = api_response.total_entries
        return total_entries
    except ApiException as e:
        logger.warning(f"Exception when calling DagRunApi: {e}")
        return None


@task
def failed_979_dags() -> list:
    """
    Find all of the failed digital_bookplate_979 DAG runs
    Returns a list of DAGRunResponse as json
    """
    client = api_client()
    api_instance = DagRunApi(client)
    total_dag_runs = total_failed_dag_runs(api_instance=api_instance)
    all_dag_runs: list = []
    if total_dag_runs is not None:
        logger.info(f"Total number of failed dag runs: {total_dag_runs}")
        all_dag_runs = get_all_failed_dag_runs(
            api_instance=api_instance,
            size=total_dag_runs,
            initial_limit=100,
            initial_offset=0,
        )
        logger.info(f"Total number of dag runs fetched: {len(all_dag_runs)}")
    else:
        logger.warning("Unable to get failed dag runs")

    return [x.to_json() for x in all_dag_runs]


@task
def clear_dag_runs(dag_runs: list) -> list:
    """
    Clears dag runs given a list of DAGRunResponses as json
    Returns list of cleared dag_run_ids
    """
    client = api_client()
    api_instance = DagRunApi(client)
    cleared_dag_runs: list = []
    for dag_run in dag_runs:
        dag_run = json.loads(dag_run)
        dag_id = dag_run.get("dag_id", "digital_bookplate_979")
        dag_run_id = dag_run.get("dag_run_id")
        dag_run_clear_body = DAGRunClearBody(dry_run=False, only_failed=True)
        try:
            logger.info(f"Clearing dag run for {dag_id} {dag_run_id}")
            api_response: ResponseClearDagRun = api_instance.clear_dag_run(
                dag_id, dag_run_id, dag_run_clear_body
            )
            state = api_response.to_dict().get("state").name  # type: ignore
            logger.info(f"Dag run ID: {dag_run_id}, state: {state}")
            cleared_dag_runs.append(dag_run_id)
        except ApiException as e:
            logger.warning(f"Could not clear dag run for {dag_id} {dag_run_id}: {e}")

    return cleared_dag_runs


@task
def poll_for_979s_dags(run_ids):
    devs_email_addr = Variable.get("EMAIL_DEVS")

    logger.info(f"{len(run_ids)} failed 979 DAG runs queued")
    logger.info(run_ids)

    launch_poll_for_979_dags_email(dag_runs=run_ids, email=devs_email_addr)

    return None
