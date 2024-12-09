import pytest

from airflow.models import Variable

from unittest.mock import AsyncMock


@pytest.fixture
def mock_task_instance(mocker):
    mock_ti = mocker.MagicMock()
    mock_ti.id = "mock_fix_encumbrance"
    mock_ti.run_id = "scheduled__2024-07-29T19:00:00:00:00"
    return mock_ti


@pytest.fixture
def mock_folio_variables(monkeypatch):
    def mock_get(key, *args):
        value = None
        match key:
            case "EMAIL_DEVS":
                value = "test@stanford.edu"

            case "EMAIL_ENC_SUL":
                value = "sul@example.com"

            case "EMAIL_ENC_LAW":
                value = "law@example.com"

            case "EMAIL_ENC_LANE":
                value = "lane@example.com"

            case "OKAPI_URL":
                value = "okapi-test"

            case "FOLIO_URL":
                value = "okapi-test"

            case _:
                raise ValueError("")
        return value

    monkeypatch.setattr(Variable, "get", mock_get)


def test_fix_encumbrances_log_file_params(
    mocker, tmp_path, mock_task_instance, mock_folio_variables, monkeypatch
):
    mocker.patch(
        'libsys_airflow.plugins.folio.encumbrances.fix_encumbrances.Variable.get',
        return_value=mock_folio_variables,
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


def test_fix_encumbrances_email_subject():
    from libsys_airflow.plugins.shared.utils import _subject_with_server_name
    from libsys_airflow.plugins.folio.encumbrances.email import subject

    subj = _subject_with_server_name(subject=subject(fy_code="SUL2024"))
    assert subj == "okapi-test - Fix Encumbrances for SUL2024"


def test_email_to(mocker):
    mocker.patch(
        'libsys_airflow.plugins.shared.utils.send_email_with_server_name',
        return_value=None,
    )

    from libsys_airflow.plugins.folio.encumbrances.email import email_to

    to_addresses = email_to(fy_code='SUL2025')
    assert to_addresses == ['test@stanford.edu', 'sul@example.com']
