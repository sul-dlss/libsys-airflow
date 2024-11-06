import copy
import pytest  # noqa

from bs4 import BeautifulSoup

from libsys_airflow.plugins.orafin.emails import (
    generate_ap_error_report_email,
    generate_ap_paid_report_email,
    generate_excluded_email,
    generate_invoice_error_email,
    generate_summary_email,
)

from test_payments import invoice_dict, invoice_lines, vendor

business_invoice = {
    "id": "00e0b6e7-a336-49e6-9e24-d4c23c4345fd",
    "accountingCode": "",
    "acqUnitIds": ["c74ceb20-33fb-4b50-914e-a056db67feea"],
    "invoiceDate": "2024-07-17T00:00:00.000+00:00",
    'folioInvoiceNo': "",
    'subTotal': 0.0,
    'total': 0.0,
    "lines": invoice_lines,
    "vendorId": "d7b8ee4b-93c5-4395-90fa-dcc04d26477b",
    "vendorInvoiceNo": "242428ZP1",
    "vendor": vendor,
}

law_invoice = {
    "id": "7b8d6012-e2eb-41c5-82b4-5e14f29a6e04",
    "accountingCode": "",
    "acqUnitIds": ["556eb26f-dbea-41c1-a1de-9a88ad950d95"],
    "invoiceDate": "2024-07-17T00:00:00.000+00:00",
    'folioInvoiceNo': "",
    'subTotal': 0.0,
    'total': 0.0,
    "lines": invoice_lines,
    "vendorId": "d7b8ee4b-93c5-4395-90fa-dcc04d26477b",
    "vendorInvoiceNo": "242428ZP1",
    "vendor": vendor,
}


def mock_retrieve_invoice_task_xcom_pull(**kwargs):
    key = kwargs.get("key")

    output = []
    match key:
        case "cancelled":
            output.extend(
                [
                    {
                        'SupplierNumber': 31134,
                        'SupplierName': 'GOBI LIBRARY SERVICES',
                        'PaymentNumber': 3262656,
                        'PaymentDate': '03/29/2024',
                        'PaymentAmount': 14038.19,
                        'InvoiceNum': '2024-988810 16009',
                        'InvoiceDate': '02/28/2024',
                        'InvoiceAmt': 380.0,
                        'AmountPaid': 380.0,
                        'PoNumber': None,
                        "invoice_id": "759dfefa-a2d4-4977-bf31-d11da1ba1fb0",
                    },
                    {
                        'SupplierNumber': 619422,
                        'SupplierName': 'CONTINUING EDUCATION OF THE BAR CALIFORNIA',
                        'PaymentNumber': 2412482,
                        'PaymentDate': '03/28/2024',
                        'PaymentAmount': 1301.81,
                        'InvoiceNum': '11142139 16497',
                        'InvoiceDate': '02/22/2024',
                        'InvoiceAmt': 476.33,
                        'AmountPaid': 476.33,
                        'PoNumber': None,
                        'invoice_id': "0332b649-4415-4a63-8a6e-4b0b16b51ab0",
                    },
                    {
                        'SupplierNumber': 2685,
                        'SupplierName': 'AUX AMATEURS DE LIVRES',
                        'PaymentNumber': 2412515,
                        'PaymentDate': '03/28/2024',
                        'PaymentAmount': 5079.42,
                        'InvoiceNum': 'F240201919 15471',
                        'InvoiceDate': '02/06/2024',
                        'InvoiceAmt': 725.45,
                        'AmountPaid': 725.45,
                        'PoNumber': None,
                        'invoice_id': "6c583579-146c-453b-aa05-e1ce0d8365cd",
                    },
                ]
            )

        case "missing":
            output.append(
                {
                    'SupplierNumber': 12580,
                    'SupplierName': 'OTTO HARRASSOWITZ GMBH AND CO KG',
                    'PaymentNumber': 3262272,
                    'PaymentDate': '03/28/2024',
                    'PaymentAmount': 0.0,
                    'InvoiceNum': '39327 16568',
                    'InvoiceDate': '03/05/2024',
                    'InvoiceAmt': -622.02,
                    'AmountPaid': -622.02,
                    'PoNumber': None,
                }
            )

        case "paid":
            output.extend(
                [
                    {
                        'SupplierNumber': 597416,
                        'SupplierName': 'BRILL',
                        'PaymentNumber': 3246272,
                        'PaymentDate': '03/13/2024',
                        'PaymentAmount': 3310.3,
                        'InvoiceNum': '1074695 15496',
                        'InvoiceDate': '02/12/2024',
                        'InvoiceAmt': 3310.3,
                        'AmountPaid': 3310.3,
                        'PoNumber': None,
                        'invoice_id': "9cf2899a-c7a6-4101-bf8e-c5996ded5fd1",
                    },
                    {
                        'SupplierNumber': 917465,
                        'SupplierName': 'EDITIO ALTERA',
                        'PaymentNumber': 3246686,
                        'PaymentDate': '03/13/2024',
                        'PaymentAmount': 20250.0,
                        'InvoiceNum': '1406 15205',
                        'InvoiceDate': '02/12/2024',
                        'InvoiceAmt': 5400.0,
                        'AmountPaid': 5400.0,
                        'PoNumber': None,
                        'invoice_id': "e2886f5c-f7f7-4d26-aa32-afc62e2d554c",
                    },
                ]
            )

    return output


def test_generate_ap_error_report_email(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name"
    )

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
        'test@stanford.edu',
    ]

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    h2s = html_body.find_all("h2")

    assert h2s[0].text == "Missing Invoices"
    assert h2s[1].text == "Cancelled Invoices"
    assert h2s[2].text == "Already Paid Invoices"

    tables = html_body.find_all("table")
    missing_trs = tables[0].find_all("tr")
    assert len(missing_trs) == 2
    assert missing_trs[0].find_all('th')[5].text.startswith("InvoiceNum")
    assert (
        missing_trs[1]
        .find_all('td')[1]
        .text.startswith("OTTO HARRASSOWITZ GMBH AND CO KG")
    )

    cancelled_trs = tables[1].find_all("tr")
    assert len(cancelled_trs) == 4
    assert cancelled_trs[0].find_all('th')[-1].text.startswith("Invoice URL")
    assert (
        cancelled_trs[2].find_all('td')[-1].find('a').get('href')
        == "http://folio.stanford.edu/invoice/view/0332b649-4415-4a63-8a6e-4b0b16b51ab0"
    )

    paid_trs = tables[2].find_all("tr")
    assert paid_trs[2].find_all('td')[3].text == "03/13/2024"


def test_generate_ap_error_report_email_options(mocker):
    def _no_output_xcom(**kwargs):
        return None

    def _no_missing_xcom(**kwargs):
        key = kwargs.get("key")
        if key.startswith("cancelled"):
            return [{"invoice_id": "759dfefa-a2d4-4977-bf31-d11da1ba1fb0"}]
        if key.startswith("paid"):
            return [{"invoice_id": "9cf2899a-c7a6-4101-bf8e-c5996ded5fd1"}]
        return None

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name"
    )

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
                },
                {
                    "id": "de3eabab-94c8-4616-9192-7f7b1483e157",
                    "vendorInvoiceNo": "56-23478",
                    "acqUnitIds": ["bd6c5f05-9ab3-41f7-8361-1c1e847196d3"],
                    "accountingCode": "071724FEEDER",
                },
            ]

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name"
    )

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    task_instance = mocker.MagicMock()
    task_instance.xcom_pull = _paid_xcom_pull

    total_invoices = generate_ap_paid_report_email(
        "http://folio.stanford.edu", task_instance
    )

    assert total_invoices == 2
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
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name"
    )

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
            {"invoice": business_invoice, "reason": "Zero subtotal"},
            {"invoice": law_invoice, "reason": "Amount split"},
            {"invoice": invoice_dict, "reason": "Amount split"},
            {"invoice": invoice_dict, "reason": "Zero subtotal"},
            {"invoice": invoice_dict, "reason": "Future invoice date"},
            {"invoice": invoice_dict, "reason": "Amount split"},
            {"invoice": invoice_dict, "reason": "Fiscal year not current"},
        ],
        "https://folio.stanford.edu",
    )

    assert mock_send_email.call_count == 3

    sul_call = mock_send_email.mock_calls[0].call_list()
    bis_call = mock_send_email.mock_calls[1].call_list()
    law_call = mock_send_email.mock_calls[2].call_list()

    assert bis_call[0][2]['subject'] == 'Rejected Invoices for Business'
    assert law_call[0][2]['subject'] == 'Rejected Invoices for LAW'

    assert sul_call[0][2]['to'] == [
        'test@stanford.edu',
        'test@stanford.edu',
    ]

    html_body = BeautifulSoup(sul_call[0][2]['html_content'], 'html.parser')

    found_h2s = html_body.findAll("h2")
    assert found_h2s[0].text == "Amount split"
    list_items = html_body.findAll("li")
    assert "Vendor Invoice Number: 242428ZP1" in list_items[0].text
    anchor = html_body.find("a")
    assert anchor.text == "Invoice line number: 1"

    assert found_h2s[1].text == "Zero subtotal"
    assert "Vendor Invoice Number: 242428ZP1" in list_items[2].text

    assert found_h2s[2].text == "Future invoice date"
    assert found_h2s[3].text == "Fiscal year not current"


def test_generate_invoice_error_email(mocker):
    def mock_xcom_pull(*args, **kwargs):
        return {
            'SupplierNumber': '610612',
            'SupplierName': 'ASKART INC',
            'PaymentNumber': '2402586',
            'PaymentDate': '01/25/2024',
            'PaymentAmount': '3500',
            'InvoiceNum': '149449_20231024 14198',
            'InvoiceDate': '10/24/2023',
            'InvoiceAmt': '3500',
            'AmountPaid': '3500',
            'PoNumber': None,
        }

    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name"
    )

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    task_instance = mocker.MagicMock()

    task_instance.xcom_pull = mock_xcom_pull

    invoice_uuid = "63550e23-968d-43d3-9bd8-a2d6d60ff1a3"

    generate_invoice_error_email(
        invoice_uuid, "http://folio.stanford.edu", task_instance
    )

    assert mock_send_email.called

    assert len(mock_send_email.call_args[1]['to']) == 4

    assert mock_send_email.call_args[1]['to'][1] == "test@stanford.edu"

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]['html_content'], 'html.parser'
    )

    assert html_body.find("a").text == invoice_uuid

    table_rows = html_body.findAll("tr")

    ap_report_row_tds = table_rows[1].findAll("td")

    assert ap_report_row_tds[0].text == "3500"
    assert ap_report_row_tds[1].text == "2402586"
    assert ap_report_row_tds[2].text == "01/25/2024"


def test_generate_summary_email(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name"
    )

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
