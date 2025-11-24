import pytest  # noqa

from bs4 import BeautifulSoup
from airflow.models import Variable

from libsys_airflow.plugins.data_exports.email import (
    generate_no_holdings_instances_email,
    generate_multiple_oclc_identifiers_email,
    generate_oclc_new_marc_errors_email,
    failed_transmission_email,
    send_confirmation_email,
)


@pytest.fixture
def mock_folio_variables(monkeypatch):
    def mock_get(key, *args):
        value = None
        match key:
            case "FOLIO_URL":
                value = "folio-test"

            case "EMAIL_DEVS":
                value = "test@stanford.edu"

            case "OCLC_EMAIL_BUS":
                value = "bus@example.com"

            case "OCLC_EMAIL_HOOVER":
                value = "hoover@example.com"

            case "OCLC_EMAIL_LANE":
                value = "lane@example.com"

            case "OCLC_EMAIL_LAW":
                value = "law@example.com"

            case "OCLC_EMAIL_SUL":
                value = "sul@example.com"

            case "OKAPI_URL":
                value = "okapi-test"

            case _:
                raise ValueError("")
        return value

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def mock_dag_run(mocker):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual_2022-03-05"
    dag_run.dag = mocker.stub(name="dag")
    dag_run.dag.dag_id = "send_vendor_records"

    return dag_run


def test_missing_holdings_instances_email(mocker, mock_folio_variables):
    mock_send_email = mocker.MagicMock()

    mocker.patch.multiple(
        "libsys_airflow.plugins.shared.utils",
        send_email=mock_send_email,
        is_production=lambda: False,
    )

    mocker.patch(
        "libsys_airflow.plugins.data_exports.email.is_production",
        return_value=True,
    )

    generate_no_holdings_instances_email.function(
        report="/opt/airflow/data-export-files/oclc/reports/missing_holdings/2025-03-04T23:15:35.345579.html"
    )

    assert mock_send_email.call_count == 1

    report_body = BeautifulSoup(
        mock_send_email.call_args_list[0][1]['html_content'], 'html.parser'
    )

    report_link = report_body.find("a")
    assert report_link.text == "2025-03-04T23:15:35.345579.html"


def test_no_missing_holdings_instances_email(mocker, mock_folio_variables, caplog):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email_with_server_name"
    )
    generate_no_holdings_instances_email.function(report=None)
    assert "All instances have holdings records" in caplog.text


def test_multiple_oclc_email(mocker, mock_folio_variables):

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

    generate_multiple_oclc_identifiers_email.function(
        reports={
            "STF": "/opt/airflow/data-export-files/oclc/reports/STF/multiple_oclc_numbers/2024-11-05T23:26:11.316254.html",
            "HIN": "/opt/airflow/data-export-files/oclc/reports/HIN/multiple_oclc_numbers/2024-11-05T23:26:12.316254.html",
            "S7Z": "/opt/airflow/data-export-files/oclc/reports/S7Z/multiple_oclc_numbers/2024-11-05T23:26:12.316254.html",
        }
    )
    assert mock_send_email.call_count == 3

    sul_html_body = BeautifulSoup(
        mock_send_email.call_args_list[0][1]['html_content'], 'html.parser'
    )

    sul_report_link = sul_html_body.find("a")

    assert sul_report_link.text == "2024-11-05T23:26:11.316254.html"

    assert mock_send_email.call_args_list[1][1]["to"] == ['test@stanford.edu']
    assert mock_send_email.call_args_list[1][1]["subject"].endswith(
        "Multiple OCLC Identifiers Hoover"
    )


def test_no_multiple_oclc_code_email(mocker, mock_folio_variables, caplog):
    mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email_with_server_name"
    )

    generate_multiple_oclc_identifiers_email.function(reports={})

    assert "No multiple OCLC Identifiers" in caplog.text


def test_prod_oclc_email(mocker, mock_folio_variables):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email_with_server_name"
    )

    mocker.patch(
        "libsys_airflow.plugins.data_exports.email.is_production",
        return_value=True,
    )

    generate_multiple_oclc_identifiers_email.function(
        reports={
            "CASUM": "/opt/airflow/data-export-files/oclc/reports/CASUM/multiple_oclc_numbers/2024-11-05T23:26:12.316254.html",
            "HIN": "/opt/airflow/data-export-files/oclc/reports/HIN/multiple_oclc_numbers/2024-11-05T23:26:12.316254.html",
            "RCJ": "/opt/airflow/data-export-files/oclc/reports/RCJ/multiple_oclc_numbers/2024-11-05T23:26:12.316254.html",
        }
    )
    assert mock_send_email.called

    assert (
        mock_send_email.call_args_list[1][1]["subject"]
        == "Review Instances with Multiple OCLC Identifiers Hoover"
    )
    assert mock_send_email.call_args_list[1][1]['to'] == [
        "hoover@example.com",
        "test@stanford.edu",
    ]


def test_no_failed_transmission_email(mock_dag_run, caplog):
    files = []
    failed_transmission_email.function(files, dag_run=mock_dag_run)
    assert "No failed files to send in email" in caplog.text


def test_failed_transmission_email(mocker, mock_dag_run, mock_folio_variables, caplog):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email_with_server_name"
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
        == "http://localhost:8080/dags/send_vendor_records/grid?dag_run_id=manual_2022-03-05"
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


def test_generate_oclc_new_marc_errors_email(
    mocker, mock_dag_run, mock_folio_variables
):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email_with_server_name"
    )

    error_reports = {
        "STF": "/opt/airflow/data-export-files/oclc/reports/STF/new_marc_errors/2024-11-26T23:26:11.316254.html",
        "HIN": "/opt/airflow/data-export-files/oclc/reports/HIN/new_marc_errors/2024-11-26T23:26:12.316254.html",
        "S7Z": "/opt/airflow/data-export-files/oclc/reports/S7Z/new_marc_errors/2024-11-26T23:26:12.316254.html",
    }

    generate_oclc_new_marc_errors_email(error_reports)

    assert mock_send_email.call_count == 3

    assert mock_send_email.call_args_list[0][1]["subject"].endswith("SUL")
    assert mock_send_email.call_args_list[1][1]["subject"].endswith("Hoover")
    assert mock_send_email.call_args_list[2][1]["subject"].endswith("Business")


def test_failed_full_dump_transmission_email(
    mocker, mock_dag_run, mock_folio_variables
):
    mock_dag_run.dag.dag_id = "send_all_records"

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email_with_server_name"
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
        == "http://localhost:8080/dags/send_all_records/grid?dag_run_id=manual_2022-03-05"
    )


def test_upload_confirmation_email(mocker, mock_folio_variables):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email_with_server_name"
    )
    send_confirmation_email.function(
        vendor="backstage",
        record_id_kind="new",
        number_of_ids=150,
        uploaded_filename="backstage_ids_2024-12-01.csv",
        user_email="backstage@example.com",
    )

    assert mock_send_email.called
    assert (
        mock_send_email.call_args[1]["subject"] == "Upload Confirmation for Data Export"
    )
    assert (
        mock_send_email.call_args[1]["to"] == "test@stanford.edu,backstage@example.com"
    )

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )
    assert (
        html_body.find("p").text
        == "Your file backstage_ids_2024-12-01.csv was successfully submitted for export to backstage as new."
    )
    assert html_body.findAll("p")[1].text == "Number of IDs submitted: 150"


def test_upload_no_confirmation_email(mocker, mock_folio_variables):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.data_exports.email.send_email_with_server_name"
    )
    send_confirmation_email.function(
        vendor="backstage",
        record_id_kind="new",
        number_of_ids=150,
        uploaded_filename="backstage_ids_2024-12-01.csv",
        user_email=None,
    )

    assert mock_send_email.called

    assert mock_send_email.call_args[1]["to"] == "test@stanford.edu"
