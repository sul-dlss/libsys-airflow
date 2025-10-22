import pytest

from datetime import datetime
from unittest.mock import MagicMock

from airflow.sdk import Variable

from libsys_airflow.plugins.digital_bookplates.dag_979_retries import (
    failed_979_dags,
    run_failed_979_dags,
)


def month():
    return datetime.now().month


def uuids():
    return [
        "47bf71da-5ca4-496d-a34e-fbc121cd3f1b",
        "ddb75fc0-1018-4508-bb17-18a1eea57ccf",
        "399c38dd-51f2-4d96-82b1-82a0e5da7de0",
        "a483d67a-aae5-48d6-a9cc-e9a061010560",
        "d062b41a-6809-4f1b-830f-48a3f95df98e",
    ]


def mock_dag_runs():
    mock_dag_runs = []

    def mock_get_state():
        return 'failed'

    def mock_get_task_instance(*args):
        task_instance = MagicMock()
        task_instance.xcom_pull = mock_xcom_pull
        return task_instance

    for id, idx in enumerate(uuids()):
        mock_dag_run = MagicMock()
        mock_dag_run.get_state = mock_get_state
        mock_dag_run.run_id = f"scheduled__2024-{month()}-{idx}"
        mock_dag_run.dag.dag_id = "digital_bookplate_979"
        mock_dag_run.conf = {"druids_for_instance_id": {id: {}}}
        mock_dag_run.get_task_instance = mock_get_task_instance
        mock_dag_runs.append(mock_dag_run)

    return mock_dag_runs


def mock_xcom_pull(*args, **kwargs):
    return {
        uuids()[-1]: [
            {
                'fund_name': 'KLEINH',
                'druid': 'vy482pt7540',
                'image_filename': 'vy482pt7540_00_0001.jp2',
                'title': 'The Herbert A. Klein Book Fund',
            }
        ]
    }


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key, *args):
        return "test@example.com"

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def mock_dag_bag(mocker):
    def mock_get_dag(dag_id: str):
        return mocker.MagicMock()

    dag_bag = mocker.MagicMock()
    dag_bag.get_dag = mock_get_dag
    return dag_bag


@pytest.fixture
def mock_dag(mocker):
    def mock_clear_dags(dags: list):
        return mocker.MagicMock()

    dag = mocker.MagicMock()
    dag.clear_dags = mock_clear_dags
    return dag


def test_find_failed_979_dags(mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_retries.DagRun.find",
        return_value=mock_dag_runs(),
    )

    failed_dags = failed_979_dags.function()
    assert len(failed_dags["digital_bookplate_979s"]) == 5
    assert f"Found: scheduled__2024-{month()}-4" in caplog.text


def test_run_failed_979_dags(mocker, mock_variable, mock_dag_bag, mock_dag, caplog):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_retries.DagRun.get_dag",
        return_value=mock_dag,
    )
    dag = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.bookplates.DagBag",
        return_value=mock_dag,
    )
    dag_bag = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_retries.DagBag",
        return_value=mock_dag_bag,
    )

    dag_runs = {"digital_bookplate_979s": mock_dag_runs()}
    assert len(dag_runs) > 0
    run_failed_979_dags.function(dag_runs=dag_runs)

    assert dag_bag.called
    assert dag.called
    assert "Clearing 5 failed 979 DAG runs" in caplog.text
