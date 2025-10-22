import pytest

from airflow.sdk import Variable
from libsys_airflow.plugins.authority_control.data_import import run_folio_data_import


@pytest.fixture
def mock_folio_variables(monkeypatch):
    def mock_get(key, *args):
        value = None
        match key:
            case "OKAPI_URL":
                value = "folio-test"

            case "FOLIO_USER":
                value = "libsys"

            case "FOLIO_PASSWORD":
                value = "password"
        return value

    monkeypatch.setattr(Variable, "get", mock_get)


def test_data_import(mock_folio_variables):
    bash_operator = run_folio_data_import("test.mrc", "test_profile")

    assert bash_operator.task_id == "run_folio_data_import"
    assert bash_operator.bash_command.startswith(
        "python3 -m folio_data_import --record-type MARC21 --gateway_url $gateway_url"
    )
    assert bash_operator.env["gateway_url"] == "folio-test"
