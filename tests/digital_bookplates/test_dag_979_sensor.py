from unittest.mock import MagicMock

# import pytest

from libsys_airflow.plugins.digital_bookplates.dag_979_sensor import DAG979Sensor


def mock_dag_run():
    def mock_get_state():
        return 'success'

    mock_dag_run = MagicMock()
    mock_dag_run.get_state = mock_get_state
    mock_dag_run.conf = {
        "druids_for_instance_id": {"d55f7f1b-9512-452c-98ff-5e2be9dcdb16": {}}
    }
    return [mock_dag_run]


def test_dag_979_sensor_no_dags():
    sensor = DAG979Sensor(
        task_id="poll-979-dags", dag_runs=["manual__2024-10-17"], poke_interval=10.0
    )
    result = sensor.poke(context={})

    assert result is False


def test_dag_979_sensor(mocker):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_sensor.DagRun.find",
        return_value=mock_dag_run(),
    )
    sensor = DAG979Sensor(
        task_id="poll-979-dags", dag_runs=["manual__2024-10-17"], poke_interval=10.0
    )
    result = sensor.poke(context={})
    assert result is True
