import pytest

from airflow.sdk import Variable

from libsys_airflow.plugins.digital_bookplates.dag_979_retries import (
    failed_979_dags,
    run_ids,
    clear_failed_add_marc_tags_to_record,
    poll_for_979s_dags,
)


@pytest.fixture
def mock_dag_list_runs():
    return [
        {
            "dag_id": "digital_bookplate_979",
            "run_id": "manual__2025-11-06T09:40:00+00:00",
            "state": "failed",
            "run_after": "2025-11-06T19:40:00+00:00",
            "logical_date": "2025-11-06T09:40:00+00:00",
            "start_date": "2025-11-06T21:47:50.247185+00:00",
            "end_date": "2025-11-06T21:47:51.278468+00:00",
        },
        {
            "dag_id": "digital_bookplate_979",
            "run_id": "manual__2025-11-06T00:40:00+00:00",
            "state": "failed",
            "run_after": "2025-11-06T09:40:00+00:00",
            "logical_date": "2025-11-06T00:40:00+00:00",
            "start_date": "2025-11-06T21:47:48.169719+00:00",
            "end_date": "2025-11-06T21:47:49.227595+00:00",
        },
    ]


@pytest.fixture
def mock_run_ids():
    return ["manual__2025-11-06T09:40:00+00:00", "manual__2025-11-06T00:40:00+00:00"]


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key, *args):
        return "test@example.com"

    monkeypatch.setattr(Variable, "get", mock_get)


def test_find_failed_979_dags(mocker, mock_dag_list_runs):
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_retries.BashOperator",
        return_value=mock_dag_list_runs,
    )

    failed_dags = failed_979_dags()
    assert len(failed_dags) == 2


def test_run_ids(mock_dag_list_runs, caplog):
    failed_dag_run_ids = run_ids.function(mock_dag_list_runs)
    assert isinstance(failed_dag_run_ids, list)
    assert failed_dag_run_ids.pop() == "manual__2025-11-06T00:40:00+00:00"
    assert "Found: manual__2025-11-06T09:40:00+00:00" in caplog.text


def test_clear_failed_add_marc_tags_to_record(caplog):
    clear_failed_add_marc_tags_to_record.function()
    assert (
        "Clearing failed add_marc_tags_to_record tasks for digital_bookplate_979 DAG runs."
        in caplog.text
    )


def test_poll_for_979s_dags(mocker, mock_run_ids, mock_variable, caplog):
    poll_for_979s_dags.function(mock_run_ids)
    mocker.patch(
        "libsys_airflow.plugins.digital_bookplates.dag_979_retries.Variable.get",
        return_value=mock_variable,
    )
    assert (
        f"{len(mock_run_ids)} failed 979 DAG runs queued: {mock_run_ids}" in caplog.text
    )
