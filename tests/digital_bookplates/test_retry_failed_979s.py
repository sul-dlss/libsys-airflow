import pytest

from airflow.sdk import Variable
from unittest.mock import MagicMock

from mocks import (  # noqa
    MockAirflowApiClientConfig,
    MockAirflowApiClient,
)

from libsys_airflow.plugins.digital_bookplates.dag_979_retries import (
    failed_979_dags,
    clear_dag_runs,
    poll_for_979s_dags,
)


@pytest.fixture
def mock_client_config():
    return MockAirflowApiClientConfig()


@pytest.fixture
def mock_api_client():
    return MockAirflowApiClient(configuration=MockAirflowApiClientConfig())


@pytest.fixture(params=["get_dag_runs", "clear_dag_run"])
def mock_api_instance(request):
    api_request = request.param
    api_instance = MagicMock()

    if api_request == "get_dag_runs":
        mock_response = MagicMock(
            dag_runs=[
                MagicMock(
                    dag_id="digital_bookplate_979",
                    dag_run_id="manual__2025-11-06T09:40:00+00:00",
                ),
                MagicMock(
                    dag_id="digital_bookplate_979",
                    dag_run_id="manual__2025-11-06T00:40:00+00:00",
                ),
            ],
            total_entries=2,
        )
        api_instance.get_dag_runs.return_value = mock_response

    elif api_request == "clear_dag_run":
        mock_state = MagicMock()
        mock_state.name = "QUEUED"

        mock_response = MagicMock()
        mock_response.to_dict.return_value = {"state": mock_state}

        api_instance = MagicMock()
        api_instance.clear_dag_run.return_value = mock_response

    return api_instance


@pytest.fixture
def mock_clear_dag_runs_op_kwargs():
    return [
        "{\"dag_id\": \"digital_bookplate_979\", \"dag_run_id\": \"manual__2025-11-06T09:40:00+00:00\"}",
        "{\"dag_id\": \"digital_bookplate_979\", \"dag_run_id\": \"manual__2025-11-06T00:40:00+00:00\"}",
    ]


@pytest.fixture
def mock_cleared_dag_runs():
    return ["manual__2025-11-06T09:40:00+00:00", "manual__2025-11-06T00:40:00+00:00"]


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key, *args):
        return "test@example.com"

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.mark.parametrize("mock_api_instance", ["get_dag_runs"], indirect=True)
def test_find_failed_979_dags(mocker, mock_api_client, mock_api_instance, caplog):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_retries.api_client",
        return_value=mock_api_client,
    )
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_retries.DagRunApi",
        return_value=mock_api_instance,
    )

    failed_dags = failed_979_dags.function()
    assert len(failed_dags) == 2
    assert "Total number of dag runs fetched: 2" in caplog.text


@pytest.mark.parametrize("mock_api_instance", ["clear_dag_run"], indirect=True)
def test_clear_dag_runs(
    mocker, mock_api_client, mock_api_instance, mock_clear_dag_runs_op_kwargs, caplog
):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_retries.api_client",
        return_value=mock_api_client,
    )
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_retries.DagRunApi",
        return_value=mock_api_instance,
    )

    cleared_dag_runs = clear_dag_runs.function(dag_runs=mock_clear_dag_runs_op_kwargs)
    assert (
        "Clearing dag run for digital_bookplate_979 manual__2025-11-06T" in caplog.text
    )
    assert "state: QUEUED" in caplog.text
    assert len(cleared_dag_runs) == 2


def test_poll_for_979s_dags(mocker, mock_cleared_dag_runs, mock_variable, caplog):
    mock_email = mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_retries.launch_poll_for_979_dags_email"
    )
    poll_for_979s_dags.function(mock_cleared_dag_runs)

    assert f"{len(mock_cleared_dag_runs)} failed 979 DAG runs queued" in caplog.text
    mock_email.assert_called_once_with(
        dag_runs=mock_cleared_dag_runs, email="test@example.com"
    )
