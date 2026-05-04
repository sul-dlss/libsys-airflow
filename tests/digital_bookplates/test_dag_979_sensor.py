import pytest

from unittest.mock import MagicMock
from airflow_client.client.rest import ApiException

from mocks import (  # noqa
    MockAirflowApiClientConfig,
    MockAirflowApiClient,
)
from libsys_airflow.plugins.digital_bookplates.dag_979_sensor import DAG979Sensor


def mock_api_instance():
    api_instance = MagicMock()

    mock_response = MagicMock()
    mock_response.dag_id = "digital_bookplate_979"
    mock_response.dag_run_id = "manual__2024-10-17"
    mock_response.conf = {
        "druids_for_instance_id": {"d55f7f1b-9512-452c-98ff-5e2be9dcdb16": {}}
    }
    mock_response.state.name = 'SUCCESS'

    api_instance.get_dag_run.return_value = mock_response

    return api_instance


def mock_api_exception():
    api_instance = MagicMock()

    mock_exception = ApiException(status=404, reason="Not Found")
    api_instance.get_dag_run.side_effect = mock_exception

    return api_instance


@pytest.fixture
def mock_client_config():
    return MockAirflowApiClientConfig()


@pytest.fixture
def mock_api_client():
    return MockAirflowApiClient(configuration=MockAirflowApiClientConfig())


def test_dag_979_sensor_no_dags(mock_api_client, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_sensor.api_client",
        return_value=mock_api_client,
    )
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_sensor.airflow_client.client.DagRunApi",
        return_value=mock_api_exception(),
    )
    sensor = DAG979Sensor(
        task_id="poll-979-dags", dag_runs=["manual__2024-10-17"], poke_interval=10.0
    )
    result = sensor.poke(context={})

    assert result is False
    assert (
        "No dag run found for digital_bookplate_979 with run ID manual__2024-10-17"
        in caplog.text
    )


def test_dag_979_sensor(mock_api_client, mocker):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_sensor.api_client",
        return_value=mock_api_client,
    )
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_sensor.airflow_client.client.DagRunApi",
        return_value=mock_api_instance(),
    )
    sensor = DAG979Sensor(
        task_id="poll-979-dags", dag_runs=["manual__2024-10-17"], poke_interval=10.0
    )
    result = sensor.poke(context={})
    assert result is True
    assert sensor.dag_runs['manual__2024-10-17']["url"].endswith(
        "digital_bookplate_979/runs/manual__2024-10-17"
    )
