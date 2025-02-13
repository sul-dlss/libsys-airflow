import pytest


from airflow.models import Variable

from libsys_airflow.plugins.authority_control import email_report


@pytest.fixture
def mock_folio_variables(monkeypatch):

    def mock_get(key, *args):
        value = None
        match key:
            case "FOLIO_URL":
                value = "folio-test"

            case "EMAIL_DEVS":
                value = "sul-unicorn-devs@lists.stanford.edu"

        return value

    monkeypatch.setattr(Variable, "get", mock_get)


def test_email_report(mocker, mock_folio_variables):

    mock_send_email = mocker.MagicMock()

    mocker.patch.multiple(
        "libsys_airflow.plugins.shared.utils",
        send_email=mock_send_email,
        is_production=lambda: False,
    )

    mocker.patch(
        "libsys_airflow.plugins.data_exports.email.is_production",
        return_value=False,
    )

    email_report("Ran folio-data-import")

    assert mock_send_email.call_count == 1
