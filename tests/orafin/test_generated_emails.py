import copy
import pytest  # noqa

from bs4 import BeautifulSoup

from libsys_airflow.plugins.orafin.emails import (
    generate_excluded_email,
    generate_summary_email,
)
from test_payments import invoice_dict, invoice_lines, vendor


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
            invoice_dict,
        ],
        "https://folio.stanford.edu",
    )

    assert mock_send_email.called
    assert mock_send_email.call_args[1]['to'] == ['test@stanford.edu']

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    assert html_body.find("h2").text == "Amount Split"
    list_items = html_body.findAll("li")
    assert "Vendor Invoice Number: 242428ZP1" in list_items[0].text
    anchor = html_body.find("a")
    assert anchor.text == "Invoice line number: 1"


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
    assert mock_send_email.call_args[1]['to'] == ['test@stanford.edu']

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    assert html_body.find("h2").text == "Approved Invoices Sent to AP"
    list_items = html_body.findAll("li")
    assert "Vendor Invoice Number: 242428ZP1" in list_items[0].text
    anchor = html_body.find("a")
    assert anchor.text == "Vendor Invoice Number: 242428ZP1"
