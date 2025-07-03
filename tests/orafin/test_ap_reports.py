import httpx
import pytest  # noqa

from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from mocks import MockTaskInstance

from libsys_airflow.plugins.orafin.reports import (
    ap_server_options,
    extract_rows,
    filter_files,
    find_reports,
    retrieve_invoice,
    retrieve_reports,
    retrieve_voucher,
    remove_reports,
    update_invoice,
    update_voucher,
)


@pytest.fixture
def mock_folio_client(mocker):
    def mock_get(*args, **kwargs):
        match args[0]:
            case """/invoice/invoices?query=(folioInvoiceNo == "10103")""":
                return [
                    {
                        "id": "3cf0ebad-6e86-4374-a21d-daf2227b09cd",
                        "status": "Approved",
                    }
                ]

            case """/invoice/invoices?query=(folioInvoiceNo == "10156")""":
                return [
                    {'id': '587c922a-5be1-4de8-a268-2a5859d62779', "status": "Paid"}
                ]

            case """/invoice/invoices?query=(folioInvoiceNo == "10204")""":
                return [
                    {
                        'id': "f8d51ddc-b47c-4f83-ad7d-e60ac2081a9a",
                        "status": "Cancelled",
                    }
                ]

            case """/invoice/invoices?query=(folioInvoiceNo == "379529")""":
                return []

            case """/invoice/invoices?query=(folioInvoiceNo == "10157")""":
                return [
                    {"id": "91c0dd9d-d906-4f08-8321-2a2f58a9a35f"},
                    {"id": "bcc5b35c-3e89-4c48-b721-9ab0cbda91a9"},
                ]

            case "/voucher-storage/vouchers?query=(invoiceId==3cf0ebad-6e86-4374-a21d-daf2227b09cd)":
                return [
                    {
                        'id': '3f94f17b-3251-4eb0-849a-d57a76ac3f03',
                        'status': 'Awaiting payment',
                    }
                ]

            case "/voucher-storage/vouchers?query=(invoiceId==587c922a-5be1-4de8-a268-2a5859d62779)":
                return [{"id": "d49924fd-6153-4894-bdbf-997126b0a55", 'status': 'Paid'}]

            case "/voucher-storage/vouchers?query=(invoiceId==3379cf1d-dd47-4f7f-9b04-7ace791e75c8)":
                return []

            case "/voucher-storage/vouchers?query=(invoiceId==e2e8344d-2ad6-44f2-bf56-f3cd04f241b3)":
                return [
                    {'id': 'b6f0407c-4929-4831-8f2b-ef1aa5a26163'},
                    {'id': '0321fbc6-8714-411a-9619-9c2b43e0df05'},
                ]

    def mock_put(*args, **kwargs):
        if args[0].endswith("3cf0ebad-6e86-4374-a21d-daf2227b09cd"):
            return httpx.Response(status_code=204)
        elif args[0].endswith("b13c879f-7f5e-49e6-a522-abf04f66fa1b"):
            raise httpx.HTTPStatusError(
                "Internal Server Error",
                request=httpx.Request("PUT", "https://okapi.stanford.edu"),
                response=httpx.Response(status_code=500),
            )
        elif args[0].startswith(
            "/voucher/vouchers/e681116d-68ce-419e-aab6-3562759a7fab"
        ):
            return httpx.Response(status_code=204)
        else:
            raise httpx.HTTPStatusError(
                "Internal Server Error",
                request=httpx.Request("PUT", "https://okapi.stanford.edu"),
                response=httpx.Response(status_code=500),
            )

    mock_client = mocker.MagicMock()
    mock_client.folio_get = mock_get
    mock_client.folio_put = mock_put
    return mock_client


@pytest.fixture
def mock_current_context(mocker, monkeypatch):
    def mock_get_current_context():
        context = mocker.MagicMock()
        context.get = lambda arg: {"ti": MockTaskInstance()}
        return context

    monkeypatch.setattr(
        "libsys_airflow.plugins.orafin.reports.get_current_context",
        mock_get_current_context,
    )


report = [
    "SupplierNumber\tSupplierName\tPaymentNumber\tPaymentDate\tPaymentAmount\tInvoiceNum\tInvoiceDate\tInvoiceAmt\tAmountPaid\tPoNumber",
    "910092\tALVARADO, JANET MARY\t2384230\t09/19/2023\t50000\tALVARADOJM09052023 10103\t08/23/2021\t50000\t50000\t",
    "001470\tAMERICAN MATHEMATICAL SOCIETY\t3098367\t09/02/2023\t11405.42\t2991432678 379587\t08/03/2023\t11405.42\t11405.42\t",
]


def test_extract_rows(tmp_path):
    airflow = tmp_path / "airflow"
    orafin_reports = airflow / "orafin-files/reports/"
    orafin_reports.mkdir(parents=True)
    existing_csv = orafin_reports / "xxdl_ap_payment_09282023161640.csv"

    with existing_csv.open('w+') as fo:
        for row in report:
            fo.write(f"{row}\n")

    invoices, dag_trigger = extract_rows(str(existing_csv))

    assert len(invoices) == 2
    assert invoices[0]["SupplierName"] == "ALVARADO, JANET MARY"
    assert invoices[0]["PaymentDate"] == "09/19/2023"
    assert invoices[0]["InvoiceAmt"] == "50000"
    assert invoices[1]["SupplierNumber"] == "001470"
    assert invoices[1]["InvoiceNum"] == "2991432678 379587"
    assert invoices[1]["AmountPaid"] == "11405.42"
    assert invoices[1]["PoNumber"] is None
    assert dag_trigger is None


def test_extract_rows_empty_file(tmp_path):
    airflow = tmp_path / "airflow"
    orafin_reports = airflow / "orafin-files/reports/"
    orafin_reports.mkdir(parents=True)
    existing_csv = orafin_reports / "xxdl_ap_payment_09282023161640.csv"
    with existing_csv.open('w+') as fo:
        fo.write(f"{report[0]}\n")

    assert existing_csv.exists()
    invoices, dag_trigger = extract_rows(str(existing_csv))
    assert len(invoices) == 0
    assert existing_csv.exists() is False
    assert dag_trigger is None


def test_extract_rows_large_file(tmp_path):
    airflow = tmp_path / "airflow"
    orafin_reports = airflow / "orafin-files/reports/"
    orafin_reports.mkdir(parents=True)
    existing_csv = orafin_reports / "xxdl_ap_payment_112820231000.csv"
    with existing_csv.open('w+') as fo:
        fo.write(f"{report[0]}\n")
        for _ in range(1200):
            fo.write(f"{report[1]}\n")

    assert existing_csv.exists()
    invoices, dag_trigger = extract_rows(str(existing_csv))
    assert len(invoices) == 1_000
    assert isinstance(dag_trigger, TriggerDagRunOperator)
    next_csv = orafin_reports / "xxdl_ap_payment_112820231000_01.csv"
    assert next_csv.exists()


def test_filter_files(tmp_path):
    airflow = tmp_path / "airflow"
    orafin_reports = airflow / "orafin-files/reports/"
    orafin_reports.mkdir(parents=True)
    existing_csv = orafin_reports / "xxdl_ap_payment_09282023161640.csv"

    with existing_csv.open('w+') as fo:
        for row in report:
            fo.write(f"{row}\n")

    existing_reports, new_reports = filter_files(
        "xxdl_ap_payment_09282023161640.csv, xxdl_ap_payment_10212023177160.csv, xxdl_ap_payment.xml",
        airflow,
    )

    assert existing_reports == [{"file_name": "xxdl_ap_payment_09282023161640.csv"}]
    assert new_reports == [{"file_name": "xxdl_ap_payment_10212023177160.csv"}]


def test_find_reports(mocker):
    mocker.patch(
        "libsys_airflow.plugins.orafin.reports.is_production", return_value=True
    )
    bash_operator = find_reports()
    assert isinstance(bash_operator, BashOperator)
    assert bash_operator.bash_command.startswith("ssh")
    assert bash_operator.bash_command.endswith(
        "ls -m /home/of_aplib/OF1_PRD/outbound/data/*.csv | tr '\n' ' '"
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
    assert (
        "Invoice f8d51ddc-b47c-4f83-ad7d-e60ac2081a9a has been Cancelled" in caplog.text
    )


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


def test_retrieve_voucher(mock_folio_client, mock_current_context):
    voucher = retrieve_voucher(
        "3cf0ebad-6e86-4374-a21d-daf2227b09cd", mock_folio_client
    )
    assert voucher["id"] == "3f94f17b-3251-4eb0-849a-d57a76ac3f03"


def test_retrieve_paid_voucher(mock_folio_client, mock_current_context, caplog):
    retrieve_voucher("587c922a-5be1-4de8-a268-2a5859d62779", mock_folio_client)
    assert "Voucher d49924fd-6153-4894-bdbf-997126b0a55 already Paid" in caplog.text


def test_retrieve_no_voucher(mock_folio_client, mock_current_context, caplog):
    retrieve_voucher("3379cf1d-dd47-4f7f-9b04-7ace791e75c8", mock_folio_client)
    assert (
        "No voucher found for invoice 3379cf1d-dd47-4f7f-9b04-7ace791e75c8"
        in caplog.text
    )


def test_retrieve_duplicate_vouchers(mock_folio_client, mock_current_context, caplog):
    retrieve_voucher("e2e8344d-2ad6-44f2-bf56-f3cd04f241b3", mock_folio_client)
    assert (
        "Multiple vouchers b6f0407c-4929-4831-8f2b-ef1aa5a26163,0321fbc6-8714-411a-9619-9c2b43e0df05"
        in caplog.text
    )


def test_retrieve_reports(mocker):
    mocker.patch(
        "libsys_airflow.plugins.orafin.reports.is_production", return_value=False
    )
    partial_bash_operator = retrieve_reports()
    bash_command = partial_bash_operator.kwargs.get("bash_command")
    assert bash_command.startswith("scp -i")
    assert bash_command.endswith("/opt/airflow/orafin-files/reports/")
    assert "intxfer-uat.stanford.edu" in bash_command
    # Test production
    mocker.patch(
        "libsys_airflow.plugins.orafin.reports.is_production", return_value=True
    )
    partial_bash_operator = retrieve_reports()
    bash_command = partial_bash_operator.kwargs.get("bash_command")
    assert "/home/of_aplib/OF1_PRD/outbound/data/$file_name" in bash_command


def test_remove_reports(mocker):
    mocker.patch(
        "libsys_airflow.plugins.orafin.reports.is_production", return_value=True
    )
    partial_bash_operator = remove_reports()
    bash_command = partial_bash_operator.kwargs.get("bash_command")
    assert bash_command.startswith("ssh")
    assert ap_server_options[0] in bash_command
    assert bash_command.endswith("rm /home/of_aplib/OF1_PRD/outbound/data/$file_name")


def test_update_invoice(mock_folio_client, caplog):
    invoice = {"id": "3cf0ebad-6e86-4374-a21d-daf2227b09cd"}
    invoice = update_invoice(invoice, mock_folio_client)
    assert (
        "Updated 3cf0ebad-6e86-4374-a21d-daf2227b09cd to status of Paid" in caplog.text
    )
    assert invoice["status"] == "Paid"


def test_update_invoice_failure(mock_folio_client):
    invoice = {"id": "b13c879f-7f5e-49e6-a522-abf04f66fa1b"}
    invoice_update_result = update_invoice(invoice, mock_folio_client)

    assert invoice_update_result is False


def test_update_voucher(mocker, mock_folio_client, caplog):
    def _xcom_pull(*args, **kwargs):
        return [
            {
                "AmountPaid": "2499.01",
                "PaymentAmount": "2498.63",
                "PaymentDate": "10/24/2023",
                "PaymentNumber": "2983835",
            }
        ]

    mock_task_instance = mocker.MagicMock()
    mock_task_instance.xcom_pull = _xcom_pull

    voucher = {
        "id": "e681116d-68ce-419e-aab6-3562759a7fab",
        "disbursementNumber": "Pending",
        "disbursementAmount": 0,
        "invoiceId": "06108f44-b03d-49c4-a2c6-1cfe3984a6d3",
    }

    changed_voucher = update_voucher(voucher, mock_task_instance, mock_folio_client)

    assert "Updated e681116d-68ce-419e-aab6-3562759a7fab" in caplog.text
    assert changed_voucher["disbursementAmount"] == "2499.01"
    assert changed_voucher["disbursementDate"] == "2023-10-24T00:00:00"
    assert changed_voucher["disbursementNumber"] == "2983835"
    assert changed_voucher["status"] == "Paid"


def test_update_voucher_failed(mocker, mock_folio_client, caplog):
    def _xcom_pull(*args, **kwargs):
        return [
            {
                "AmountPaid": "2499.01",
                "PaymentAmount": "2498.63",
                "PaymentDate": "10/24/2023",
                "PaymentNumber": "2983835",
            }
        ]

    mock_task_instance = mocker.MagicMock()
    mock_task_instance.xcom_pull = _xcom_pull

    voucher = {
        "id": "992a30bc-c65b-4514-8d11-f8d325e2f10a",
        "disbursementNumber": "Pending",
        "disbursementAmount": 0,
        "invoiceId": "06108f44-b03d-49c4-a2c6-1cfe3984a6d3",
    }

    update_voucher(voucher, mock_task_instance, mock_folio_client)

    assert (
        "Failed to update voucher 992a30bc-c65b-4514-8d11-f8d325e2f10a" in caplog.text
    )
