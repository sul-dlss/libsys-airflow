import copy
import pytest  # noqa

from bs4 import BeautifulSoup

from libsys_airflow.plugins.orafin.emails import (
    generate_ap_error_report_email,
    generate_ap_paid_report_email,
    generate_excluded_email,
    generate_summary_email,
)

from test_payments import invoice_dict, invoice_lines, vendor


def mock_retrieve_invoice_task_xcom_pull(**kwargs):
    key = kwargs.get("key")

    output = []
    match key:
        case "cancelled":
            output.extend(
                [
                    "759dfefa-a2d4-4977-bf31-d11da1ba1fb0",
                    "0332b649-4415-4a63-8a6e-4b0b16b51ab0",
                    "6c583579-146c-453b-aa05-e1ce0d8365cd",
                ]
            )

        case "missing":
            output.append("10440")

        case "paid":
            output.extend(
                [
                    "9cf2899a-c7a6-4101-bf8e-c5996ded5fd1",
                    "e2886f5c-f7f7-4d26-aa32-afc62e2d554c",
                ]
            )

    return output


def test_generate_ap_error_report_email(mocker):
    mock_send_email = mocker.patch("libsys_airflow.plugins.orafin.emails.send_email")

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    task_instance = mocker.MagicMock()
    task_instance.xcom_pull = mock_retrieve_invoice_task_xcom_pull

    total_errors = generate_ap_error_report_email(
        "http://folio.stanford.edu", task_instance
    )

    assert total_errors == 6
    assert mock_send_email.called
    assert mock_send_email.call_args[1]['to'] == [
        'test@stanford.edu',
        'test@stanford.edu',
        'test@stanford.edu',
    ]

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    h2s = html_body.find_all("h2")

    assert h2s[0].text == "Missing Invoices"
    assert h2s[1].text == "Cancelled Invoices"
    assert h2s[2].text == "Already Paid Invoices"

    uls = html_body.find_all("ul")
    missing_lis = uls[0].find_all("li")
    assert missing_lis[0].text == "10440 found in AP Report but not in FOLIO"

    cancelled_lis = uls[1].find_all("li")
    assert (
        cancelled_lis[1].find("a").get("href")
        == "http://folio.stanford.edu/invoice/view/0332b649-4415-4a63-8a6e-4b0b16b51ab0"
    )

    paid_lis = uls[2].find_all("li")
    assert paid_lis[0].find("a").text == "Invoice 9cf2899a-c7a6-4101-bf8e-c5996ded5fd1"


def test_generate_ap_error_report_email_options(mocker):
    def _no_output_xcom(**kwargs):
        return None

    def _no_missing_xcom(**kwargs):
        key = kwargs.get("key")
        if key.startswith("cancelled"):
            return ["759dfefa-a2d4-4977-bf31-d11da1ba1fb0"]
        if key.startswith("paid"):
            return ["9cf2899a-c7a6-4101-bf8e-c5996ded5fd1"]
        return None

    mock_send_email = mocker.patch("libsys_airflow.plugins.orafin.emails.send_email")

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    no_output_task_instance = mocker.MagicMock()
    no_output_task_instance.xcom_pull = _no_output_xcom

    total_errors = generate_ap_error_report_email(
        "http://folio.stanford.edu", no_output_task_instance
    )

    assert total_errors == 0
    assert mock_send_email.called is False

    no_missing_task_instance = mocker.MagicMock()
    no_missing_task_instance.xcom_pull = _no_missing_xcom

    total_errors = generate_ap_error_report_email(
        "http://folio.stanford.edu", no_missing_task_instance
    )

    assert total_errors == 2
    assert mock_send_email.called


def test_generate_ap_paid_report_email(mocker):
    def _paid_xcom_pull(**kwargs):
        task_ids = kwargs["task_ids"]
        if task_ids.startswith("init_processing_task"):
            return "/opt/airflow/orafin-data/reports/xxdl_ap_payment_09282023161640.csv"
        if task_ids.startswith("retrieve_invoice_task"):
            return [
                {
                    "id": "9cf2899a-c7a6-4101-bf8e-c5996ded5fd1",
                    "vendorInvoiceNo": "23-24364",
                    "acqUnitIds": ["bd6c5f05-9ab3-41f7-8361-1c1e847196d3"],
                    "accountingCode": "031134FEEDER",
                }
            ]

    mock_send_email = mocker.patch("libsys_airflow.plugins.orafin.emails.send_email")

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    task_instance = mocker.MagicMock()
    task_instance.xcom_pull = _paid_xcom_pull

    total_invoices = generate_ap_paid_report_email(
        "http://folio.stanford.edu", task_instance
    )

    assert total_invoices == 1
    assert mock_send_email.called

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    paragraph = html_body.find("p")
    assert paragraph.text == "From ap report xxdl_ap_payment_09282023161640.csv"

    li = html_body.find("li")
    assert li.find("a").text == "Vendor Invoice Number: 23-24364"
    assert "031134FEEDER" not in li.find("a").text


def test_generate_excluded_email(mocker):
    mock_send_email = mocker.patch("libsys_airflow.plugins.orafin.emails.send_email")

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    new_invoice_lines = copy.deepcopy(invoice_lines)

    new_invoice_lines.append(
        {
            "id": "119d7428-d40c-42e1-a711-ab7dcd47b2de",
            "subTotal": 4000.23,
            "total": 4000.23,
            "poLineId": "b793afc-904b-4daf-94e9-9b7ac0445113",
            "invoiceLineNumber": "1",
            "adjustmentsTotal": 0.0,
            "fundDistributions": [
                {
                    "fundId": "698876aa-180c-4cb8-b865-6e91321122c8",
                    "distributionType": "amount",
                    "value": 2000.23,
                },
                {
                    "fundId": "f2a9b12e-3012-48a4-8d50-78b6123af36a",
                    "distributionType": "amount",
                    "value": 2000.00,
                },
            ],
        }
    )
    invoice_dict["lines"] = new_invoice_lines
    invoice_dict["vendor"] = vendor

    generate_excluded_email(
        [
            {"invoice": invoice_dict, "reason": "Amount split"},
            {"invoice": invoice_dict, "reason": "Zero subtotal"},
            {"invoice": invoice_dict, "reason": "Future invoice date"},
        ],
        "https://folio.stanford.edu",
    )

    assert mock_send_email.called
    assert mock_send_email.call_args[1]['to'] == [
        'test@stanford.edu',
        'test@stanford.edu',
    ]

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    found_h2s = html_body.findAll("h2")
    assert found_h2s[0].text == "Amount split"
    list_items = html_body.findAll("li")
    assert "Vendor Invoice Number: 242428ZP1" in list_items[0].text
    anchor = html_body.find("a")
    assert anchor.text == "Invoice line number: 1"

    assert found_h2s[1].text == "Zero subtotal"
    assert "Vendor Invoice Number: 242428ZP1" in list_items[2].text

    assert found_h2s[2].text == "Future invoice date"


def test_generate_summary_email(mocker):
    mock_send_email = mocker.patch("libsys_airflow.plugins.orafin.emails.send_email")

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    invoice_dict["vendor"] = vendor

    generate_summary_email(
        [
            invoice_dict,
        ],
        "https://folio.stanford.edu",
    )

    assert mock_send_email.called
    assert mock_send_email.call_args[1]['to'] == [
        'test@stanford.edu',
        'test@stanford.edu',
    ]

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    assert html_body.find("h2").text == "Approved Invoices Sent to AP"
    list_items = html_body.findAll("li")
    assert "Vendor Invoice Number: 242428ZP1" in list_items[0].text
    anchor = html_body.find("a")
    assert anchor.text == "Vendor Invoice Number: 242428ZP1"
