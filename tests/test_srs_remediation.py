import pytest  # noqa

from pytest_mock import MockerFixture

import libsys_airflow.dags.srs_remediation as srs_remediation

from libsys_airflow.dags.srs_remediation import (
    complete_snapshot,
    start_srs_check_remediation,
)

from tests.mocks import (  # noqa
    MockFOLIOClient,
    mock_okapi_success,
    mock_file_system,
    mock_dag_run,
)
from tests.test_remediate import mock_audit_database


@pytest.fixture
def mock_context(mocker: MockerFixture) -> MockerFixture:
    def mock_get(*args):
        return {"iteration": "manual_2023-07-21"}

    mocker.get = mock_get  # type: ignore
    return mocker


@pytest.fixture
def mock_folio_client_func(monkeypatch):
    monkeypatch.setattr(srs_remediation, '_folio_client', MockFOLIOClient)


def test_complete_snapshot(mock_folio_client_func, mock_okapi_success):  # noqa
    status_code = complete_snapshot.function(snapshot="abacedasdfsdf")
    assert status_code == 200


def test_start_srs_check_remediation(
    mock_context, mock_file_system, mock_dag_run  # noqa
):
    dag_run = mock_dag_run()
    dag_run.run_id = "manual_2023-07-21"
    (mock_file_system[0] / "migration/iterations/manual_2023-07-21/results").mkdir(
        parents=True
    )

    mock_audit_database(dag_run, mock_file_system)
    params = start_srs_check_remediation.function(
        context=mock_context, airflow=mock_file_system[0]
    )

    assert params["iteration"] == "manual_2023-07-21"
    assert params["results_dir"].endswith("manual_2023-07-21/results/")
