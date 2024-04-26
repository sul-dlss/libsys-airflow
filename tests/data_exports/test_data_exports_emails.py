import pytest  # noqa

from bs4 import BeautifulSoup

from libsys_airflow.plugins.data_exports.email import (
    Variable,
    generate_multiple_oclc_identifiers_email,
    generate_oclc_transmission_email
)


def test_multiple_oclc_email(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email"
    )

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    generate_multiple_oclc_identifiers_email(
        """[
            (
                "ae0b6949-6219-51cd-9a61-7794c2081fe7",
                "STF",
                ["(OCoLC-M)21184692", "(OCoLC-I)272673749"],
            ),
            (
                "0221724f-2bca-497b-8d42-6786295e7173",
                "HIN",
                ["(OCoLC-M)99087632", "(OCoLC-I)889220055"],
            ),
        ]"""
    )
    assert mock_send_email.called

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    list_items = html_body.find_all("li")

    assert (
        list_items[0]
        .find("a")
        .get("href")
        .endswith("/inventory/viewsource/ae0b6949-6219-51cd-9a61-7794c2081fe7")
    )

    assert "(OCoLC-M)21184692" in list_items[0].text

    assert (
        list_items[1]
        .find("a")
        .get("href")
        .endswith("/inventory/viewsource/0221724f-2bca-497b-8d42-6786295e7173")
    )


def test_no_multiple_oclc_code_email(mocker, caplog):
    mocker.patch("libsys_airflow.plugins.data_exports.email.send_email")

    mocker.patch(
        "libsys_airflow.plugins.data_exports.email.Variable.get",
        return_value="test@stanford.edu",
    )

    generate_multiple_oclc_identifiers_email([])

    assert "No multiple OCLC Identifiers" in caplog.text


@pytest.fixture
def mock_airflow_variables(monkeypatch, mocker):
    def mock_get(*args):
        key = args[0]
        match key:
            case "EMAIL_DEVS":
                return "dev_email@stanford.edu"
            
            case "FOLIO_URL":
                return "https://folio.stanford.edu"
            
            case "ORAFIN_TO_EMAIL_LAW":
                return "law_email@stanford.edu"
            
            case "ORAFIN_TO_EMAIL_SUL":
                return "sul_email@stanford.edu"
            
            case _:
                return ""

    monkeypatch.setattr(Variable, "get", mock_get)

def test_successful_oclc_export_email(mocker, mock_airflow_variables, caplog):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email"
    )

    generate_oclc_transmission_email("success", [], "STF")

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    assert mock_send_email.called

    
def test_failures_oclc_export_email(mocker, mock_airflow_variables, caplog):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email"
    )

    generate_oclc_transmission_email("failure", [], "RCJ")

    assert mock_send_email.called
