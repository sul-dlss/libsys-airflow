import pytest  # noqa

from airflow_client.client import DagRunState, TaskInstanceState

from libsys_airflow.plugins.shared.dag_runs import active_dag_runs


def _task_instance(mocker, task_id, state):
    return mocker.MagicMock(task_display_name=task_id, state=state)


@pytest.fixture
def mock_apis(mocker):
    mocker.patch("libsys_airflow.plugins.shared.dag_runs.api_client")
    dag_run_api = mocker.patch(
        "libsys_airflow.plugins.shared.dag_runs.DagRunApi"
    ).return_value
    task_instance_api = mocker.patch(
        "libsys_airflow.plugins.shared.dag_runs.TaskInstanceApi"
    ).return_value
    return dag_run_api, task_instance_api


def test_active_dag_runs(mocker, mock_apis):
    mock_dag_run_api, mock_task_instance_api = mock_apis

    first_run = mocker.MagicMock(
        dag_run_id="first-run", conf={"key": "value"}, state=DagRunState.RUNNING
    )
    second_run = mocker.MagicMock(
        dag_run_id="second-run", conf=None, state=DagRunState.QUEUED
    )
    mock_dag_run_api.get_dag_runs.side_effect = lambda dag_id, **kwargs: (
        mocker.MagicMock(dag_runs=[first_run if dag_id == "dag_a" else second_run])
    )
    task_instances = {
        "first-run": [
            _task_instance(mocker, "setup", TaskInstanceState.SUCCESS),
            _task_instance(mocker, "mapped_task", TaskInstanceState.RUNNING),
            _task_instance(mocker, "mapped_task", TaskInstanceState.RUNNING),
            _task_instance(mocker, "finish", None),
        ],
        "second-run": [],
    }
    mock_task_instance_api.get_task_instances.side_effect = (
        lambda dag_id, run_id, **kwargs: mocker.MagicMock(
            task_instances=task_instances[run_id]
        )
    )

    runs = active_dag_runs(["dag_a", "dag_b"])

    assert runs == [
        {
            "dag_id": "dag_a",
            "dag_run_id": "first-run",
            "state": "running",
            "conf": {"key": "value"},
            "finished_tasks": 1,
            "total_tasks": 4,
            "running_tasks": ["mapped_task"],
        },
        {
            "dag_id": "dag_b",
            "dag_run_id": "second-run",
            "state": "queued",
            "conf": {},
            "finished_tasks": 0,
            "total_tasks": 0,
            "running_tasks": [],
        },
    ]
    for call in mock_dag_run_api.get_dag_runs.call_args_list:
        assert call.kwargs["state"] == ["queued", "running"]


def test_active_dag_runs_none_active(mock_apis):
    mock_dag_run_api, mock_task_instance_api = mock_apis
    mock_dag_run_api.get_dag_runs.return_value.dag_runs = []

    assert active_dag_runs(["dag_a"]) == []
    mock_task_instance_api.get_task_instances.assert_not_called()
