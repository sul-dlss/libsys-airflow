import copy
import pytest  # noqa

from bs4 import BeautifulSoup

from libsys_airflow.plugins.orafin.emails import (
    generate_ap_error_report_email,
    generate_ap_paid_report_email,
    generate_excluded_email,
    generate_failed_dag_email,
    generate_summary_email,
    generate_voucher_error_email,
    _group_invoices_by_acqunit,
)

from test_payments import invoice_dict, invoice_lines, vendor

business_invoice = {
    "id": "00e0b6e7-a336-49e6-9e24-d4c23c4345fd",
    "accountingCode": "",
    "acqUnitIds": ["c74ceb20-33fb-4b50-914e-a056db67feea"],
    "invoiceDate": "2024-07-17T00:00:00.000+00:00",
    "folioInvoiceNo": "",
    "subTotal": 0.0,
    "total": 0.0,
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
    "folioInvoiceNo": "",
    "subTotal": 0.0,
    "total": 0.0,
    "lines": invoice_lines,
    "vendorId": "d7b8ee4b-93c5-4395-90fa-dcc04d26477b",
    "vendorInvoiceNo": "242428ZP1",
    "vendor": vendor,
}


def test_generate_ap_error_report_email(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name",
        return_value=True,
    )
    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        side_effect=lambda key: {
            "EMAIL_DEVS": "test@stanford.edu",
            "ORAFIN_TO_EMAIL_SUL": "test@stanford.edu",
            "ORAFIN_TO_EMAIL_LAW": "test@stanford.edu",
            "ORAFIN_TO_EMAIL_BUS": "test@stanford.edu",
            "FOLIO_URL": "http://folio.stanford.edu",
        }.get(key, "test@stanford.edu"),
    )

    missing = [
        {
            "SupplierNumber": 12580,
            "SupplierName": "OTTO HARRASSOWITZ GMBH AND CO KG",
            "PaymentNumber": 3262272,
            "PaymentDate": "03/28/2024",
            "PaymentAmount": 0.0,
            "InvoiceNum": "39327 16568",
            "InvoiceDate": "03/05/2024",
            "InvoiceAmt": -622.02,
            "AmountPaid": -622.02,
            "PoNumber": None,
        }
    ]
    cancelled = [
        {
            "SupplierNumber": 31134,
            "SupplierName": "GOBI LIBRARY SERVICES",
            "PaymentNumber": 3262656,
            "PaymentDate": "03/29/2024",
            "PaymentAmount": 14038.19,
            "InvoiceNum": "2024-988810 16009",
            "InvoiceDate": "02/28/2024",
            "InvoiceAmt": 380.0,
            "AmountPaid": 380.0,
            "PoNumber": None,
            "invoice_id": "759dfefa-a2d4-4977-bf31-d11da1ba1fb0",
        },
        {
            "SupplierNumber": 619422,
            "SupplierName": "CONTINUING EDUCATION OF THE BAR CALIFORNIA",
            "PaymentNumber": 2412482,
            "PaymentDate": "03/28/2024",
            "PaymentAmount": 1301.81,
            "InvoiceNum": "11142139 16497",
            "InvoiceDate": "02/22/2024",
            "InvoiceAmt": 476.33,
            "AmountPaid": 476.33,
            "PoNumber": None,
            "invoice_id": "0332b649-4415-4a63-8a6e-4b0b16b51ab0",
        },
        {
            "SupplierNumber": 2685,
            "SupplierName": "AUX AMATEURS DE LIVRES",
            "PaymentNumber": 2412515,
            "PaymentDate": "03/28/2024",
            "PaymentAmount": 5079.42,
            "InvoiceNum": "F240201919 15471",
            "InvoiceDate": "02/06/2024",
            "InvoiceAmt": 725.45,
            "AmountPaid": 725.45,
            "PoNumber": None,
            "invoice_id": "6c583579-146c-453b-aa05-e1ce0d8365cd",
        },
    ]
    already_paid = [
        {
            "SupplierNumber": 597416,
            "SupplierName": "BRILL",
            "PaymentNumber": 3246272,
            "PaymentDate": "03/13/2024",
            "PaymentAmount": 3310.3,
            "InvoiceNum": "1074695 15496",
            "InvoiceDate": "02/12/2024",
            "InvoiceAmt": 3310.3,
            "AmountPaid": 3310.3,
            "PoNumber": None,
            "invoice_id": "9cf2899a-c7a6-4101-bf8e-c5996ded5fd1",
        },
        {
            "SupplierNumber": 917465,
            "SupplierName": "EDITIO ALTERA",
            "PaymentNumber": 3246686,
            "PaymentDate": "03/13/2024",
            "PaymentAmount": 20250.0,
            "InvoiceNum": "1406 15205",
            "InvoiceDate": "02/12/2024",
            "InvoiceAmt": 5400.0,
            "AmountPaid": 5400.0,
            "PoNumber": None,
            "invoice_id": "e2886f5c-f7f7-4d26-aa32-afc62e2d554c",
        },
    ]
    failed_updates = [
        {
            "InvoiceNum": "F260516273 39892",
            "invoice_id": "48e9c078-ac3a-41a0-9bee-262e785df5f5",
        }
    ]

    result = generate_ap_error_report_email(
        missing, cancelled, already_paid, failed_updates
    )

    assert result is True
    assert mock_send_email.called
    assert mock_send_email.call_args[1]["to"] == [
        "test@stanford.edu",
        "test@stanford.edu",
        "test@stanford.edu",
        "test@stanford.edu",
    ]

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    h2s = html_body.find_all("h2")

    assert h2s[0].text == "Missing Invoices"
    assert h2s[1].text == "Cancelled Invoices"
    assert h2s[2].text == "Already Paid Invoices"
    assert h2s[3].text == "Error Updating Invoices"

    tables = html_body.find_all("table")
    missing_trs = tables[0].find_all("tr")
    assert len(missing_trs) == 2
    assert missing_trs[0].find_all("th")[5].text.startswith("InvoiceNum")
    assert (
        missing_trs[1]
        .find_all("td")[1]
        .text.startswith("OTTO HARRASSOWITZ GMBH AND CO KG")
    )

    cancelled_trs = tables[1].find_all("tr")
    assert len(cancelled_trs) == 4
    assert cancelled_trs[0].find_all("th")[-1].text.startswith("Invoice URL")
    assert (
        cancelled_trs[2].find_all("td")[-1].find("a").get("href")
        == "http://folio.stanford.edu/invoice/view/0332b649-4415-4a63-8a6e-4b0b16b51ab0"
    )

    paid_trs = tables[2].find_all("tr")
    assert paid_trs[2].find_all("td")[3].text == "03/13/2024"

    failed_update_trs = tables[3].find_all("tr")
    assert (
        failed_update_trs[1].find_all("td")[-1].find("a").get("href")
        == "http://folio.stanford.edu/invoice/view/48e9c078-ac3a-41a0-9bee-262e785df5f5"
    )


def test_generate_ap_error_report_email_no_errors(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name"
    )

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    # Test with empty lists
    result = generate_ap_error_report_email([], [], [], [])
    assert result is True
    assert mock_send_email.called


def test_generate_ap_paid_report_email(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name"
    )

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        side_effect=lambda key: {
            "EMAIL_DEVS": "test@stanford.edu",
            "ORAFIN_TO_EMAIL_SUL": "test@stanford.edu",
            "ORAFIN_TO_EMAIL_LAW": "test@stanford.edu",
            "ORAFIN_TO_EMAIL_BUS": "test@stanford.edu",
            "FOLIO_URL": "http://folio.stanford.edu",
        }.get(key, "test@stanford.edu"),
    )

    invoices = [
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

    report_path = "/opt/airflow/orafin-data/reports/xxdl_ap_payment_09282023161640.csv"

    result = generate_ap_paid_report_email(invoices, report_path)

    assert result == {"sul": True}
    assert mock_send_email.called

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    paragraph = html_body.find("p")
    assert paragraph.text == "From ap report xxdl_ap_payment_09282023161640.csv"

    li = html_body.find("li")
    assert li.find("a").text == "Vendor Invoice Number: 23-24364"
    assert "031134FEEDER" not in li.find("a").text


def test_group_invoices_by_acqunit():
    invoice = {
        "id": "9cf2899a-c7a6-4101-bf8e-c5996ded5fd1",
        "vendorInvoiceNo": "23-24364",
        "acqUnitIds": ["bd6c5f05-9ab3-41f7-8361-1c1e847196d3"],
        "accountingCode": "031134FEEDER",
    }

    grouped_acqunits = _group_invoices_by_acqunit([invoice])
    assert isinstance(grouped_acqunits["bd6c5f05-9ab3-41f7-8361-1c1e847196d3"], list)

    invoices = [
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
    grouped_acqunits = _group_invoices_by_acqunit(invoices)
    assert len(grouped_acqunits["bd6c5f05-9ab3-41f7-8361-1c1e847196d3"]) == 2


def test_generate_ap_paid_report_email_no_invoices(mocker):
    mocker.patch("libsys_airflow.plugins.orafin.emails.send_email_with_server_name")

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    result = generate_ap_paid_report_email(
        [], "/opt/airflow/orafin-data/reports/xxdl_ap_payment.csv"
    )

    assert result == {}


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
        ]
    )

    assert mock_send_email.call_count == 3

    sul_call = mock_send_email.mock_calls[0].call_list()
    bis_call = mock_send_email.mock_calls[1].call_list()
    law_call = mock_send_email.mock_calls[2].call_list()

    assert bis_call[0][2]["subject"] == "Rejected Invoices for Business"
    assert law_call[0][2]["subject"] == "Rejected Invoices for LAW"

    assert sul_call[0][2]["to"] == [
        "test@stanford.edu",
        "test@stanford.edu",
    ]

    html_body = BeautifulSoup(sul_call[0][2]["html_content"], "html.parser")

    found_h2s = html_body.find_all("h2")
    assert found_h2s[0].text == "Amount split"
    list_items = html_body.find_all("li")
    assert "Vendor Invoice Number: 242428ZP1" in list_items[0].text
    anchor = html_body.find("a")
    assert anchor.text == "Invoice line number: 1"

    assert found_h2s[1].text == "Zero subtotal"
    assert "Vendor Invoice Number: 242428ZP1" in list_items[2].text

    assert found_h2s[2].text == "Future invoice date"
    assert found_h2s[3].text == "Fiscal year not current"


def test_generate_failed_dag_email(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name"
    )

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    mock_context = mocker.MagicMock()
    mock_dag_run = mocker.MagicMock()
    mock_dag_run.run_id = "mock_instance_run_id"
    mock_dag_run.dag_id = "ap_payment_report"
    mock_context.get.return_value = mock_dag_run

    generate_failed_dag_email(mock_context, "http://airflow-stanford.edu")

    assert mock_send_email.called

    assert mock_send_email.call_args[1]["to"][0] == "test@stanford.edu"

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    h1 = html_body.find("h1")

    assert h1.text == "ap_payment_report DAG Failed"


def test_generate_summary_email(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name"
    )

    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    invoice_dict["vendor"] = vendor

    generate_summary_email([invoice_dict])

    assert mock_send_email.called
    assert mock_send_email.call_args[1]["to"] == [
        "test@stanford.edu",
        "test@stanford.edu",
    ]

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    assert html_body.find("h2").text == "Approved Invoices Sent to AP"
    list_items = html_body.find_all("li")
    assert "Vendor Invoice Number: 242428ZP1" in list_items[0].text
    anchor = html_body.find("a")
    assert anchor.text == "Vendor Invoice Number: 242428ZP1"


def test_generate_voucher_error_email(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name",
        return_value=True,
    )
    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        side_effect=lambda key: {
            "EMAIL_DEVS": "test@stanford.edu",
            "ORAFIN_TO_EMAIL_SUL": "test@stanford.edu",
            "ORAFIN_TO_EMAIL_LAW": "test@stanford.edu",
            "ORAFIN_TO_EMAIL_BUS": "test@stanford.edu",
            "FOLIO_URL": "http://folio.stanford.edu",
        }.get(key, "test@stanford.edu"),
    )

    # Mock voucher objects for failed_updates
    mock_voucher1 = mocker.MagicMock()
    mock_voucher1.id = "voucher-123"
    mock_voucher1.invoiceId = "invoice-456"

    mock_voucher2 = mocker.MagicMock()
    mock_voucher2.id = "voucher-789"
    mock_voucher2.invoiceId = "invoice-101"

    missing = [
        "9cf2899a-c7a6-4101-bf8e-c5996ded5fd1",
        "de3eabab-94c8-4616-9192-7f7b1483e157",
    ]

    multiples = [
        "759dfefa-a2d4-4977-bf31-d11da1ba1fb0",
        "0332b649-4415-4a63-8a6e-4b0b16b51ab0",
    ]

    failed_updates = [mock_voucher1, mock_voucher2]

    result = generate_voucher_error_email(missing, multiples, failed_updates)

    assert result is True
    assert mock_send_email.called
    assert len(mock_send_email.call_args[1]["to"]) == 4
    assert mock_send_email.call_args[1]["subject"] == "Voucher Errors from AP Report"

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    h1 = html_body.find("h1")
    assert h1.text == "Voucher Failures from AP Report"
    h2s = html_body.find_all("h2")
    assert len(h2s) == 3
    assert h2s[0].text == "Missing Vouchers for Invoice"
    assert h2s[1].text == "Multiple Vouchers Found for Invoice"
    assert h2s[2].text == "Failed to Update Voucher for Invoice"

    # Check missing vouchers section
    uls = html_body.find_all("ul")
    missing_links = uls[0].find_all("a")
    assert len(missing_links) == 2
    assert (
        missing_links[0].get("href")
        == "http://folio.stanford.edu/invoice/view/9cf2899a-c7a6-4101-bf8e-c5996ded5fd1"
    )
    assert missing_links[0].text == "Invoice 9cf2899a-c7a6-4101-bf8e-c5996ded5fd1"
    assert (
        missing_links[1].get("href")
        == "http://folio.stanford.edu/invoice/view/de3eabab-94c8-4616-9192-7f7b1483e157"
    )

    # Check multiples section
    multiples_links = uls[1].find_all("a")
    assert len(multiples_links) == 2
    assert (
        multiples_links[0].get("href")
        == "http://folio.stanford.edu/invoice/view/759dfefa-a2d4-4977-bf31-d11da1ba1fb0"
    )
    assert multiples_links[0].text == "Invoice 759dfefa-a2d4-4977-bf31-d11da1ba1fb0"

    # Check failed updates section
    failed_links = uls[2].find_all("a")
    assert len(failed_links) == 2
    assert (
        failed_links[0].get("href")
        == "http://folio.stanford.edu/invoice/view/invoice-456/voucher/voucher-123/view"
    )
    assert failed_links[0].text == "Voucher voucher-123"
    assert (
        failed_links[1].get("href")
        == "http://folio.stanford.edu/invoice/view/invoice-101/voucher/voucher-789/view"
    )
    assert failed_links[1].text == "Voucher voucher-789"


def test_generate_voucher_error_email_empty_lists(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name",
        return_value=True,
    )
    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    # Test with all empty lists
    result = generate_voucher_error_email([], [], [])

    assert result is True
    assert mock_send_email.called

    # Check that email still contains main heading but no section content
    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )
    h1 = html_body.find("h1")
    assert h1.text == "Voucher Failures from AP Report"

    # Should have no h2 sections since all lists are empty
    h2s = html_body.find_all("h2")
    assert len(h2s) == 0


def test_generate_voucher_error_email_send_failure(mocker):
    # Mock send_email to raise an exception
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name",
        side_effect=Exception("Email sending failed"),
    )
    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    missing = ["test-invoice-id"]

    result = generate_voucher_error_email(missing, [], [])

    assert result is False
    assert mock_send_email.called


def test_generate_voucher_error_email_only_missing(mocker):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.orafin.emails.send_email_with_server_name",
        return_value=True,
    )
    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )

    missing = ["invoice-123"]
    result = generate_voucher_error_email(missing, [], [])

    assert result is True

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )

    h2s = html_body.find_all("h2")
    assert len(h2s) == 1  # Only one section should be present
    assert h2s[0].text == "Missing Vouchers for Invoice"

    # Should have one link in missing section
    ul = html_body.find("ul")
    links = ul.find_all("a")
    assert len(links) == 1
    assert links[0].text == "Invoice invoice-123"
