import pytest  # noqa

from unittest.mock import MagicMock

from libsys_airflow.plugins.orafin.ap_reports import (
    retrieve_invoice,
    retrieve_rows,
    retrieve_voucher,
)

from mocks import MockTaskInstance, mock_dag_run, mock_file_system  # noqa

report = [
    "SupplierNumber,SupplierName,PaymentNumber,PaymentDate,PaymentAmount,InvoiceNum,InvoiceDate,InvoiceAmt,AmountPaid,PoNumber",
    "910092,ALVARADO, JANET MARY,2384230,09/19/2023,50000,ALVARADOJM09052023 10103,08/23/2021,50000,50000,",
    "001470,AMERICAN MATHEMATICAL SOCIETY,3098367,09/02/2023,11405.42,2991432678 379587,08/03/2023,11405.42,11405.42,",
]


@pytest.fixture
def mock_folio_client():
    def mock_get(*args, **kwargs):
        match args[0]:
            case """/invoice/invoices?query=(folioInvoiceNo == "10103")""":
                return {
                    "invoices": [
                        {
                            "id": "3cf0ebad-6e86-4374-a21d-daf2227b09cd",
                            "status": "Approved",
                        }
                    ]
                }

            case """/invoice/invoices?query=(folioInvoiceNo == "10156")""":
                return {
                    "invoices": [
                        {'id': '587c922a-5be1-4de8-a268-2a5859d62779', "status": "Paid"}
                    ]
                }

            case """/invoice/invoices?query=(folioInvoiceNo == "10204")""":
                return {
                    "invoices": [
                        {'id': "f8d51ddc-b47c-4f83-ad7d-e60ac2081a9a", "status": "Cancelled"}
                    ]
                }

            case """/invoice/invoices?query=(folioInvoiceNo == "379529")""":
                return {"invoices": []}

            case """/invoice/invoices?query=(folioInvoiceNo == "10157")""":
                return {
                    "invoices": [
                        {"id": "91c0dd9d-d906-4f08-8321-2a2f58a9a35f"},
                        {"id": "bcc5b35c-3e89-4c48-b721-9ab0cbda91a9"},
                    ]
                }

            case "/voucher-storage/vouchers?query=(invoiceId==3cf0ebad-6e86-4374-a21d-daf2227b09cd)":
                return {
                    "vouchers": [
                        {
                            'id': '3f94f17b-3251-4eb0-849a-d57a76ac3f03',
                            'status': 'Awaiting payment',
                        }
                    ]
                }

            case "/voucher-storage/vouchers?query=(invoiceId==587c922a-5be1-4de8-a268-2a5859d62779)":
                return {
                    "vouchers": [
                        {"id": "d49924fd-6153-4894-bdbf-997126b0a55", 'status': 'Paid'}
                    ]
                }
            
            case "/voucher-storage/vouchers?query=(invoiceId==3379cf1d-dd47-4f7f-9b04-7ace791e75c8)":
                return {
                    "vouchers": []
                }
            
            case "/voucher-storage/vouchers?query=(invoiceId==e2e8344d-2ad6-44f2-bf56-f3cd04f241b3)":
                return {
                    "vouchers": [
                        {'id': 'b6f0407c-4929-4831-8f2b-ef1aa5a26163'},
                        {'id': '0321fbc6-8714-411a-9619-9c2b43e0df05'},
                    ]
                }
            


    mock_client = MagicMock()
    mock_client.get = mock_get
    return mock_client


@pytest.fixture
def mock_current_context(monkeypatch):
    def mock_get_current_context():
        context = MagicMock()
        context.get = lambda arg: {"ti": MockTaskInstance()}
        return context

    monkeypatch.setattr(
        "libsys_airflow.plugins.orafin.ap_reports.get_current_context",
        mock_get_current_context,
    )


def test_retrieve_invoice(mock_folio_client, mock_current_context):
    row = {"InvoiceNum": "ALVARADOJM09052023 10103"}
    invoice = retrieve_invoice(row, mock_folio_client)
    assert invoice['id'] == "3cf0ebad-6e86-4374-a21d-daf2227b09cd"


def test_retrieve_paid_invoice(mock_folio_client, mock_current_context, caplog):
    row = {'InvoiceNum': '1K3M-7P1J-HL9M 10156'}
    retrieve_invoice(row, mock_folio_client)
    assert "Invoice 587c922a-5be1-4de8-a268-2a5859d62779 already Paid" in caplog.text


def test_retrieve_cancelled_invoice(mock_folio_client, mock_current_context, caplog):
    row = {"InvoiceNum": "4785466 10204"}
    retrieve_invoice(row, mock_folio_client)
    assert "Invoice f8d51ddc-b47c-4f83-ad7d-e60ac2081a9a has been Cancelled" in caplog.text


def test_retrieve_no_invoice(mock_folio_client, mock_current_context, caplog):
    row = {"InvoiceNum": "11FC-KXN3-P7XG 379529"}
    retrieve_invoice(row, mock_folio_client)
    assert "No Invoice found for folioInvoiceNo 379529" in caplog.text


def test_retrieve_duplicate_invoices(mock_folio_client, mock_current_context, caplog):
    row = {"InvoiceNum": "1WGV-71F4-4D4V 10157"}
    retrieve_invoice(row, mock_folio_client)
    assert (
        "Multiple invoices 91c0dd9d-d906-4f08-8321-2a2f58a9a35f,bcc5b35c-3e89-4c48-b721-9ab0cbda91a9"
        in caplog.text
    )


def test_retrieve_rows(mock_file_system):  # noqa
    airflow = mock_file_system[0]

    mock_csv_report = "xxdl_ap_payment_10022023.csv"
    orafin_reports = airflow / "orafin-files/reports"
    orafin_reports.mkdir(parents=True)

    with (orafin_reports / mock_csv_report).open('w+') as fo:
        for line in report:
            fo.write(f"{line}\n")

    report_records = retrieve_rows(mock_csv_report, str(airflow))

    assert report_records[0]["SupplierName"] == "ALVARADO, JANET MARY"
    assert report_records[1]["PaymentDate"] == "09/02/2023"


def test_retrieve_voucher(mock_folio_client, mock_current_context):
    voucher = retrieve_voucher(
        "3cf0ebad-6e86-4374-a21d-daf2227b09cd", mock_folio_client
    )
    assert voucher["id"] == "3f94f17b-3251-4eb0-849a-d57a76ac3f03"


def test_retrieve_paid_voucher(mock_folio_client, mock_current_context, caplog):
    retrieve_voucher(
        "587c922a-5be1-4de8-a268-2a5859d62779", mock_folio_client
    )
    assert "Voucher d49924fd-6153-4894-bdbf-997126b0a55 already Paid" in caplog.text


def test_retrieve_no_voucher(mock_folio_client, mock_current_context, caplog):
    retrieve_voucher("3379cf1d-dd47-4f7f-9b04-7ace791e75c8", mock_folio_client)
    assert "No voucher found for invoice 3379cf1d-dd47-4f7f-9b04-7ace791e75c8" in caplog.text

def test_retrieve_duplicate_vouchers(mock_folio_client, mock_current_context, caplog):
    retrieve_voucher("e2e8344d-2ad6-44f2-bf56-f3cd04f241b3", mock_folio_client)
    assert "Multiple vouchers b6f0407c-4929-4831-8f2b-ef1aa5a26163,0321fbc6-8714-411a-9619-9c2b43e0df05" in caplog.text