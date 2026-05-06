import pytest

from unittest.mock import MagicMock
from airflow_client.client.rest import ApiException

from mocks import (  # noqa
    MockAirflowApiClientConfig,
    MockAirflowApiClient,
)
from libsys_airflow.plugins.digital_bookplates.dag_979_sensor import DAG979Sensor


@pytest.fixture(params=["success", "not_found", "mixed"])
def mock_api_instance(request):
    test_type = request.param
    api_instance = MagicMock()

    if test_type == "success":
        mock_response = MagicMock()
        mock_response.dag_id = "digital_bookplate_979"
        mock_response.dag_run_id = "manual__2024-10-17"
        mock_response.conf = {
            "druids_for_instance_id": {"d55f7f1b-9512-452c-98ff-5e2be9dcdb16": {}}
        }
        mock_response.state.name = 'SUCCESS'
        api_instance.get_dag_run.return_value = mock_response

    elif test_type == "not_found":
        api_instance.get_dag_run.side_effect = ApiException(
            status=404, reason="Not Found"
        )

    elif test_type == "mixed":
        # For mixed, create a success response and a 500 exception
        mock_success_response = MagicMock()
        mock_success_response.dag_id = "digital_bookplate_979"
        mock_success_response.dag_run_id = "manual__2024-10-17"
        mock_success_response.conf = {
            "druids_for_instance_id": {"d55f7f1b-9512-452c-98ff-5e2be9dcdb16": {}}
        }
        mock_success_response.state.name = 'SUCCESS'

        # side_effect with a list: first call returns success, second raises exception
        api_instance.get_dag_run.side_effect = [
            mock_success_response,
            ApiException(status=500, reason="Internal Server Error"),
        ]

    return api_instance


@pytest.fixture
def mock_client_config():
    return MockAirflowApiClientConfig()


@pytest.fixture
def mock_api_client():
    return MockAirflowApiClient(configuration=MockAirflowApiClientConfig())


@pytest.mark.parametrize("mock_api_instance", ["not_found"], indirect=True)
def test_dag_979_sensor_no_dags(mock_api_client, mock_api_instance, mocker, caplog):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_sensor.api_client",
        return_value=mock_api_client,
    )
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_sensor.airflow_client.client.DagRunApi",
        return_value=mock_api_instance,
    )
    sensor = DAG979Sensor(
        task_id="poll-979-dags", dag_runs=["manual__2024-10-17"], poke_interval=10.0
    )
    result = sensor.poke(context={})

    assert result is True
    assert (
        "No dag run found for digital_bookplate_979 with run ID manual__2024-10-17"
        in caplog.text
    )


@pytest.mark.parametrize("mock_api_instance", ["mixed"], indirect=True)
def test_dag_979_sensor_mixed_responses(
    mock_api_client, mock_api_instance, mocker, caplog
):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_sensor.api_client",
        return_value=mock_api_client,
    )
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_sensor.airflow_client.client.DagRunApi",
        return_value=mock_api_instance,
    )
    sensor = DAG979Sensor(
        task_id="poll-979-dags",
        dag_runs=["manual__2024-10-17", "manual__2024-10-18"],
        poke_interval=10.0,
    )
    result = sensor.poke(context={})
    assert result is True
    assert sensor.dag_runs["manual__2024-10-17"]["state"] == "success"
    assert (
        sensor.dag_runs["manual__2024-10-17"]["instances"][0]["uuid"]
        == "d55f7f1b-9512-452c-98ff-5e2be9dcdb16"
    )
    assert sensor.dag_runs["manual__2024-10-18"]["state"] is None
    assert "Exception when calling DagRunApi for manual__2024-10-18" in caplog.text


@pytest.mark.parametrize("mock_api_instance", ["success"], indirect=True)
def test_dag_979_sensor(mock_api_client, mock_api_instance, mocker):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_sensor.api_client",
        return_value=mock_api_client,
    )
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_sensor.airflow_client.client.DagRunApi",
        return_value=mock_api_instance,
    )
    sensor = DAG979Sensor(
        task_id="poll-979-dags", dag_runs=["manual__2024-10-17"], poke_interval=10.0
    )
    result = sensor.poke(context={})
    assert result is True
    assert sensor.dag_runs['manual__2024-10-17']["url"].endswith(
        "digital_bookplate_979/runs/manual__2024-10-17"
    )
