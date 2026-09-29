from typing import Iterable

from airflow_client.client import DagRunApi, TaskInstanceApi

from libsys_airflow.plugins.shared.airflow_api_client import api_client

ACTIVE_DAG_RUN_STATES = ["queued", "running"]

# Task instance states that count toward a DAG run's progress indicator.
FINISHED_TASK_STATES = {"success", "skipped", "failed", "upstream_failed", "removed"}


def active_dag_runs(dag_ids: Iterable[str]) -> list[dict]:
    """
    Lists queued/running DAG runs for dag_ids with their task progress, for
    plugin pages' progress indicators. Each run includes its conf so callers
    can map runs back to what triggered them.
    Progress counts task instances, so a mapped task only adds to the total
    once it expands.

    To show these runs with the shared templates/_dag_run_progress.html macro,
    callers replace conf with "progress_keys", a list of keys identifying the
    rows the run affects. Every page using the macro tags each of those rows
    with the same generic attribute, data-progress-key="<key>", and marks the
    cell that shows the bar with data-progress-cell.
    """
    runs = []
    with api_client() as airflow_api_client:
        dag_run_api = DagRunApi(airflow_api_client)
        task_instance_api = TaskInstanceApi(airflow_api_client)
        for dag_id in dag_ids:
            dag_runs = dag_run_api.get_dag_runs(dag_id, state=ACTIVE_DAG_RUN_STATES)
            for dag_run in dag_runs.dag_runs:
                task_instances = task_instance_api.get_task_instances(
                    dag_id, dag_run.dag_run_id, limit=100
                ).task_instances
                runs.append(
                    {
                        "dag_id": dag_id,
                        "dag_run_id": dag_run.dag_run_id,
                        "state": dag_run.state.value,
                        "conf": dag_run.conf or {},
                        "finished_tasks": sum(
                            1
                            for ti in task_instances
                            if ti.state in FINISHED_TASK_STATES
                        ),
                        "total_tasks": len(task_instances),
                        "running_tasks": sorted(
                            {
                                ti.task_display_name
                                for ti in task_instances
                                if ti.state == "running"
                            }
                        ),
                    }
                )
    return runs
