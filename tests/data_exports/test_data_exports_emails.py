import pytest  # noqa

from bs4 import BeautifulSoup
from airflow.models import Variable

from libsys_airflow.plugins.data_exports.email import (
    generate_multiple_oclc_identifiers_email,
    failed_transmission_email,
)


@pytest.fixture
def mock_folio_variables(monkeypatch):
    def mock_get(key):
        value = None
        match key:
            case "AIRFLOW__WEBSERVER__BASE_URL":
                value = "example.com"

            case "EMAIL_DEVS":
                value = "test@stanford.edu"

            case _:
                raise ValueError("")
        return value

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def mock_dag_run(mocker):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual_2022-03-05"
    dag_run.id = "send_vendor_records"

    return dag_run


def test_multiple_oclc_email(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email"
    )

    mocker.patch(
        "libsys_airflow.plugins.data_exports.email.Variable.get",
        return_value="test@stanford.edu",
    )

    generate_multiple_oclc_identifiers_email(
        [
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
        ]
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


def test_no_failed_transmission_email(mock_dag_run, caplog):
    files = []
    failed_transmission_email.function(files, dag_run=mock_dag_run)
    assert "No failed files to send in email" in caplog.text


def test_failed_transmission_email(mocker, mock_dag_run, mock_folio_variables, caplog):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email"
    )

    files = [
        "data-export-files/some-vendor/marc-files/updates/1234.mrc",
        "data-export-files/some-vendor/marc-files/updates/5678.mrc",
    ]

    failed_transmission_email.function(files, dag_run=mock_dag_run)

    assert "Generating email of failed to transmit files" in caplog.text

    assert mock_send_email.called

    assert (
        mock_send_email.call_args[1]["subject"]
        == "Failed File Transmission for send_vendor_records manual_2022-03-05"
    )

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    assert (
        html_body.find("h2").text == "Failed to Transmit Files for send_vendor_records"
    )
    assert html_body.find("a").text == "manual_2022-03-05"

    assert (
        html_body.find("a").attrs["href"]
        == "https://example.com/dags/send_vendor_records/grid?dag_run_id=manual_2022-03-05"
    )

    list_items = html_body.findAll("li")

    assert (
        list_items[0].text.strip()
        == "data-export-files/some-vendor/marc-files/updates/1234.mrc"
    )
    assert (
        list_items[1].text.strip()
        == "data-export-files/some-vendor/marc-files/updates/5678.mrc"
    )


def test_failed_full_dump_transmission_email(
    mocker, mock_dag_run, mock_folio_variables
):
    mock_dag_run.id = "send_all_records"

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email"
    )

    files = [
        "data-export-files/some-vendor/marc-files/updates/1234.mrc",
        "data-export-files/some-vendor/marc-files/updates/5678.mrc",
    ]

    failed_transmission_email.function(
        files, params={"vendor": "pod"}, dag_run=mock_dag_run
    )

    assert mock_send_email.called

    assert (
        mock_send_email.call_args[1]["subject"]
        == "Failed File Transmission for send_all_records manual_2022-03-05"
    )

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    assert (
        html_body.find("h2").text == "Failed to Transmit Files for send_all_records pod"
    )
    assert html_body.find("a").text == "manual_2022-03-05"

    assert (
        html_body.find("a").attrs["href"]
        == "https://example.com/dags/send_all_records/grid?dag_run_id=manual_2022-03-05"
    )
