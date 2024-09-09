import pytest

from unittest.mock import AsyncMock

from airflow.models import Variable


@pytest.fixture
def mock_task_instance(mocker):
    mock_ti = mocker.MagicMock()
    mock_ti.id = "mock_fix_encumbrance"
    mock_ti.run_id = "scheduled__2024-07-29T19:00:00:00:00"
    return mock_ti


@pytest.fixture
def mock_okapi(monkeypatch):
    def mock_get(*args):
        return "http://okapi-test"

    monkeypatch.setattr(Variable, "get", mock_get)


def reset_mock_okapi(monkeypatch):
    monkeypatch.delenv('OKAPI_URL', raising=False)


def test_fix_encumbrances_log_file_params(
    mocker, tmp_path, mock_task_instance, mock_okapi, monkeypatch
):
    mocker.patch(
        'libsys_airflow.plugins.folio.encumbrances.fix_encumbrances.Variable.get',
        return_value=mock_okapi,
    )

    async_mock = AsyncMock()
    mocker.patch(
        'libsys_airflow.plugins.folio.encumbrances.fix_encumbrances.run_operation',
        side_effect=async_mock,
        return_value=None,
    )
    from libsys_airflow.plugins.folio.encumbrances.fix_encumbrances_run import (
        fix_encumbrances_run,
    )

    log_path = fix_encumbrances_run(
        1,
        "SUL2024",
        "sul",
        "username",
        "password",
        airflow=tmp_path,
        task_instance=mock_task_instance,
        library="foo",
    )

    assert log_path.endswith("foo-scheduled__2024-07-29T19:00:00:00:00.log")
