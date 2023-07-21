import pytest  # noqa

from pytest_mock import MockerFixture

import libsys_airflow.dags.srs_remediation as srs_remediation

from libsys_airflow.dags.srs_remediation import (
    complete_snapshot,
    start_srs_check_remediation,
)

from tests.mocks import MockFOLIOClient, mock_okapi_success  # noqa


@pytest.fixture
def mock_context(mocker: MockerFixture) -> MockerFixture:
    def mock_get(*args):
        return {"iteration": "/opt/airflow/migration/iterations/manual_2023-07-21"}

    mocker.get = mock_get
    return mocker


@pytest.fixture
def mock_folio_client_func(monkeypatch):
    monkeypatch.setattr(srs_remediation, '_folio_client', MockFOLIOClient)


def test_complete_snapshot(mock_folio_client_func, mock_okapi_success):  # noqa
    status_code = complete_snapshot.function(snapshot="abacedasdfsdf")
    assert status_code == 200


def test_start_srs_check_remediation(mock_context):
    params = start_srs_check_remediation.function(context=mock_context)

    assert params["iteration"] == "/opt/airflow/migration/iterations/manual_2023-07-21"
    assert params["results_dir"].endswith("manual_2023-07-21/results/")
