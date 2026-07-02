import pytest
import httpx

from unittest.mock import patch
from airflow.exceptions import AirflowException, AirflowSkipException

from libsys_airflow.plugins.orafin.tasks import (
    consolidate_reports_task,
    email_invoice_errors_task,
    email_vouchers_errors_task,
    email_paid_task,
    process_folio_results_task,
    retrieve_invoice_task,
    retrieve_voucher_task,
    update_invoices_task,
    update_vouchers_task,
)


def mock_xcom_pull(**kwargs):
    key = kwargs.get("key")

    output = []
    match key:
        case "existing_reports":
            output.append({"file_name": "xxdl_ap_payment_09282023161640.csv"})

        case "new_reports":
            output.append({"file_name": "xxdl_ap_payment_10212023177160.csv"})

    return output


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
                    {"id": "587c922a-5be1-4de8-a268-2a5859d62779", "status": "Paid"}
                ]

            case """/invoice/invoices?query=(folioInvoiceNo == "10204")""":
                return [
                    {
                        "id": "f8d51ddc-b47c-4f83-ad7d-e60ac2081a9a",
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
                        "id": "3f94f17b-3251-4eb0-849a-d57a76ac3f03",
                        "status": "Awaiting payment",
                    }
                ]

            case "/voucher-storage/vouchers?query=(invoiceId==587c922a-5be1-4de8-a268-2a5859d62779)":
                return [{"id": "d49924fd-6153-4894-bdbf-997126b0a55", "status": "Paid"}]

            case "/voucher-storage/vouchers?query=(invoiceId==3379cf1d-dd47-4f7f-9b04-7ace791e75c8)":
                return []

            case "/voucher-storage/vouchers?query=(invoiceId==e2e8344d-2ad6-44f2-bf56-f3cd04f241b3)":
                return [
                    {"id": "b6f0407c-4929-4831-8f2b-ef1aa5a26163"},
                    {"id": "0321fbc6-8714-411a-9619-9c2b43e0df05"},
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
            "/voucher/vouchers/d49924fd-6153-4894-bdbf-997126b0a55"
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


@pytest.fixture(params=["found", "missing", "cancelled", "paid"])
def mock_extract_rows_task(request):
    row = {
        "SupplierNumber": "910092",
        "SupplierName": "ALVARADO, JANET MARY",
        "PaymentNumber": "2384230",
        "PaymentDate": "09/19/2023",
        "PaymentAmount": "50000",
        "InvoiceNum": "ALVARADOJM09052023 10103",
        "InvoiceDate": "08/23/2021",
        "InvoiceAmt": "50000",
        "AmountPaid": "50000",
        "PoNumber": None,
    }
    match request.param:
        case "found":
            return row
        case "missing":
            row["InvoiceNum"] = "11FC-KXN3-P7XG 379529"
            return row
        case "cancelled":
            row["InvoiceNum"] = "4785466 10204"
            return row
        case "paid":
            row["InvoiceNum"] = "1K3M-7P1J-HL9M 10156"
            return row
        case _:
            return row


@pytest.fixture(params=["found", "success", "failed", "missing", "cancelled", "paid"])
def mock_invoice_tasks(request):
    match request.param:
        case "found":
            return {
                "invoice": {
                    "id": "3cf0ebad-6e86-4374-a21d-daf2227b09cd",
                    "status": "Approved",
                },
                "3cf0ebad-6e86-4374-a21d-daf2227b09cd": {
                    "SupplierNumber": "910092",
                    "SupplierName": "ALVARADO, JANET MARY",
                    "PaymentNumber": "2384230",
                    "PaymentDate": "09/19/2023",
                    "PaymentAmount": "50000",
                    "InvoiceNum": "ALVARADOJM09052023 10103",
                    "InvoiceDate": "08/23/2021",
                    "InvoiceAmt": "50000",
                    "AmountPaid": "50000",
                    "PoNumber": None,
                },
            }
        case "success":
            return {
                "invoice": {
                    "id": "3cf0ebad-6e86-4374-a21d-daf2227b09cd",
                    "status": "Paid",
                },
                "3cf0ebad-6e86-4374-a21d-daf2227b09cd": {
                    "SupplierNumber": "910092",
                    "SupplierName": "ALVARADO, JANET MARY",
                    "PaymentNumber": "2384230",
                    "PaymentDate": "09/19/2023",
                    "PaymentAmount": "50000",
                    "InvoiceNum": "ALVARADOJM09052023 10103",
                    "InvoiceDate": "08/23/2021",
                    "InvoiceAmt": "50000",
                    "AmountPaid": "50000",
                    "PoNumber": None,
                },
                "success": {
                    "id": "3cf0ebad-6e86-4374-a21d-daf2227b09cd",
                    "status": "Paid",
                },
            }
        case "failed":
            return {
                "invoice": {
                    "id": "b13c879f-7f5e-49e6-a522-abf04f66fa1b",
                    "status": "Approved",
                },
                "b13c879f-7f5e-49e6-a522-abf04f66fa1b": {
                    "SupplierNumber": "910092",
                    "SupplierName": "ALVARADO, JANET MARY",
                    "PaymentNumber": "2384230",
                    "PaymentDate": "09/19/2023",
                    "PaymentAmount": "50000",
                    "InvoiceNum": "ALVARADOJM09052023 10103",
                    "InvoiceDate": "08/23/2021",
                    "InvoiceAmt": "50000",
                    "AmountPaid": "50000",
                    "PoNumber": None,
                },
                "failed": {
                    "SupplierNumber": "910092",
                    "SupplierName": "ALVARADO, JANET MARY",
                    "PaymentNumber": "2384230",
                    "PaymentDate": "09/19/2023",
                    "PaymentAmount": "50000",
                    "InvoiceNum": "ALVARADOJM09052023 10103",
                    "InvoiceDate": "08/23/2021",
                    "InvoiceAmt": "50000",
                    "AmountPaid": "50000",
                    "PoNumber": None,
                    "invoice_id": "b13c879f-7f5e-49e6-a522-abf04f66fa1b",
                },
            }
        case "missing":
            return {
                "missing": {
                    "SupplierNumber": "910092",
                    "SupplierName": "ALVARADO, JANET MARY",
                    "PaymentNumber": "2384230",
                    "PaymentDate": "09/19/2023",
                    "PaymentAmount": "50000",
                    "InvoiceNum": "11FC-KXN3-P7XG 379529",
                    "InvoiceDate": "08/23/2021",
                    "InvoiceAmt": "50000",
                    "AmountPaid": "50000",
                    "PoNumber": None,
                }
            }
        case "paid":
            return {
                "invoice": {
                    "id": "587c922a-5be1-4de8-a268-2a5859d62779",
                    "status": "Paid",
                },
                "587c922a-5be1-4de8-a268-2a5859d62779": {
                    "SupplierNumber": "910092",
                    "SupplierName": "ALVARADO, JANET MARY",
                    "PaymentNumber": "2384230",
                    "PaymentDate": "09/19/2023",
                    "PaymentAmount": "50000",
                    "InvoiceNum": "1K3M-7P1J-HL9M 10156",
                    "InvoiceDate": "08/23/2021",
                    "InvoiceAmt": "50000",
                    "AmountPaid": "50000",
                    "PoNumber": None,
                },
                "success": {
                    "id": "587c922a-5be1-4de8-a268-2a5859d62779",
                    "status": "Paid",
                },
            }


def test_consolidate_reports_task(mocker):
    mock_task_instance = mocker
    mock_task_instance.xcom_pull = mock_xcom_pull
    all_reports = consolidate_reports_task.function(ti=mock_task_instance)
    assert len(all_reports) == 2


@pytest.mark.parametrize("mock_extract_rows_task", ["found"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_retrieve_invoice_task(
    patched_folio_client, mock_folio_client, mock_extract_rows_task
):
    patched_folio_client.return_value = mock_folio_client
    invoice = retrieve_invoice_task.function(row=mock_extract_rows_task)
    assert invoice["invoice"]["id"] == "3cf0ebad-6e86-4374-a21d-daf2227b09cd"
    assert invoice["3cf0ebad-6e86-4374-a21d-daf2227b09cd"] == mock_extract_rows_task


@pytest.mark.parametrize("mock_extract_rows_task", ["paid"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_retrieve_paid_invoice(
    patched_folio_client, mock_folio_client, mock_extract_rows_task, caplog
):
    patched_folio_client.return_value = mock_folio_client
    invoice = retrieve_invoice_task.function(row=mock_extract_rows_task)
    assert invoice["paid"]["invoice_id"] == "587c922a-5be1-4de8-a268-2a5859d62779"
    assert "Invoice 587c922a-5be1-4de8-a268-2a5859d62779 already Paid" in caplog.text


@pytest.mark.parametrize("mock_extract_rows_task", ["cancelled"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_retrieve_cancelled_invoice(
    patched_folio_client, mock_folio_client, mock_extract_rows_task, caplog
):
    patched_folio_client.return_value = mock_folio_client
    invoice = retrieve_invoice_task.function(row=mock_extract_rows_task)
    assert invoice["cancelled"]["invoice_id"] == "f8d51ddc-b47c-4f83-ad7d-e60ac2081a9a"
    assert (
        "Invoice f8d51ddc-b47c-4f83-ad7d-e60ac2081a9a has been Cancelled" in caplog.text
    )


@pytest.mark.parametrize("mock_extract_rows_task", ["missing"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_retrieve_no_invoice(
    patched_folio_client, mock_folio_client, mock_extract_rows_task, caplog
):
    patched_folio_client.return_value = mock_folio_client
    invoice = retrieve_invoice_task.function(row=mock_extract_rows_task)
    assert invoice["missing"] == mock_extract_rows_task
    assert "No Invoice found for folioInvoiceNo 379529" in caplog.text


@pytest.mark.parametrize("mock_invoice_tasks", ["found"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_update_invoices_task(
    patched_folio_client, mock_folio_client, mock_invoice_tasks, caplog
):
    patched_folio_client.return_value = mock_folio_client
    update_result = update_invoices_task.function(mock_invoice_tasks)
    assert (
        "Updated 3cf0ebad-6e86-4374-a21d-daf2227b09cd to status of Paid" in caplog.text
    )
    assert update_result["success"]["status"] == "Paid"


@pytest.mark.parametrize("mock_invoice_tasks", ["failed"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_update_invoices_task_failure(
    patched_folio_client, mock_folio_client, mock_invoice_tasks, caplog
):
    patched_folio_client.return_value = mock_folio_client
    update_result = update_invoices_task.function(mock_invoice_tasks)
    assert (
        "Failed to update invoice b13c879f-7f5e-49e6-a522-abf04f66fa1b: Internal Server Error"
        in caplog.text
    )
    assert (
        update_result["failed"]["invoice_id"] == "b13c879f-7f5e-49e6-a522-abf04f66fa1b"
    )


@pytest.mark.parametrize("mock_invoice_tasks", ["missing"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_skip_update_invoices_task(
    patched_folio_client, mock_folio_client, mock_invoice_tasks, caplog
):
    patched_folio_client.return_value = mock_folio_client
    update_result = update_invoices_task.function(mock_invoice_tasks)
    assert "No invoice to update" in caplog.text
    assert isinstance(update_result.get("missing"), dict)
    assert update_result.get("invoice") is None


@pytest.mark.parametrize("mock_invoice_tasks", ["success"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_retrieve_voucher_task(
    patched_folio_client, mock_folio_client, mock_invoice_tasks, caplog
):
    patched_folio_client.return_value = mock_folio_client
    voucher = retrieve_voucher_task.function(mock_invoice_tasks)
    assert voucher["voucher"]["id"] == "3f94f17b-3251-4eb0-849a-d57a76ac3f03"


@pytest.mark.parametrize("mock_invoice_tasks", ["paid"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_retrieve_paid_voucher(
    patched_folio_client, mock_folio_client, mock_invoice_tasks, caplog
):
    patched_folio_client.return_value = mock_folio_client
    voucher = retrieve_voucher_task.function(mock_invoice_tasks)
    assert "Voucher d49924fd-6153-4894-bdbf-997126b0a55 already Paid" in caplog.text
    assert voucher["voucher"]["status"] == "Paid"


@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_retrieve_no_voucher(patched_folio_client, mock_folio_client, caplog):
    update_result = {
        "success": {"id": "3379cf1d-dd47-4f7f-9b04-7ace791e75c8", "status": "Paid"}
    }
    patched_folio_client.return_value = mock_folio_client
    voucher = retrieve_voucher_task.function(update_result)
    assert (
        "No voucher found for invoice 3379cf1d-dd47-4f7f-9b04-7ace791e75c8"
        in caplog.text
    )
    assert voucher["missing"] == "3379cf1d-dd47-4f7f-9b04-7ace791e75c8"


@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_retrieve_duplicate_vouchers(patched_folio_client, mock_folio_client, caplog):
    update_result = {
        "success": {"id": "e2e8344d-2ad6-44f2-bf56-f3cd04f241b3", "status": "Paid"}
    }
    patched_folio_client.return_value = mock_folio_client
    voucher = retrieve_voucher_task.function(update_result)
    assert (
        "Multiple vouchers b6f0407c-4929-4831-8f2b-ef1aa5a26163,0321fbc6-8714-411a-9619-9c2b43e0df05"
        in caplog.text
    )
    assert voucher["multiple"] == "e2e8344d-2ad6-44f2-bf56-f3cd04f241b3"


@pytest.mark.parametrize("mock_invoice_tasks", ["missing"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_skip_retrieve_voucher(
    patched_folio_client, mock_folio_client, mock_invoice_tasks
):
    patched_folio_client.return_value = mock_folio_client
    with pytest.raises(
        AirflowSkipException, match=r"Skipping voucher retrieval.*error conditions"
    ):
        retrieve_voucher_task.function(mock_invoice_tasks)


@pytest.mark.parametrize("mock_invoice_tasks", ["missing"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_skip_update_voucher(
    patched_folio_client, mock_folio_client, mock_invoice_tasks
):
    patched_folio_client.return_value = mock_folio_client
    with pytest.raises(AirflowSkipException) as exc_info:
        update_vouchers_task.function({}, mock_invoice_tasks)
    assert "Cannot update voucher" in str(exc_info.value)


@pytest.mark.parametrize("mock_invoice_tasks", ["success"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_skip_update_voucher_missing(
    patched_folio_client, mock_folio_client, mock_invoice_tasks
):
    voucher_result = {"missing": "3f94f17b-3251-4eb0-849a-d57a76ac3f03"}
    patched_folio_client.return_value = mock_folio_client
    with pytest.raises(AirflowSkipException) as exc_info:
        update_vouchers_task.function(voucher_result, mock_invoice_tasks)
    assert "Cannot update voucher" in str(exc_info.value)


@pytest.mark.parametrize("mock_invoice_tasks", ["success"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_update_vouchers_task(
    patched_folio_client, mock_folio_client, mock_invoice_tasks, caplog
):
    voucher = {
        "paid": "Voucher d49924fd-6153-4894-bdbf-997126b0a55 already Paid",
        "voucher": {
            "id": "d49924fd-6153-4894-bdbf-997126b0a55",
            "status": "Paid",
            "disbursementNumber": "Pending",
            "disbursementAmount": 0,
            "invoiceId": "3cf0ebad-6e86-4374-a21d-daf2227b09cd",
        },
    }
    patched_folio_client.return_value = mock_folio_client
    changed_voucher = update_vouchers_task.function(voucher, mock_invoice_tasks)

    assert "Updated d49924fd-6153-4894-bdbf-997126b0a55" in caplog.text
    assert changed_voucher["success"]["id"] == "d49924fd-6153-4894-bdbf-997126b0a55"
    assert changed_voucher.get("failed") is None
    assert changed_voucher["voucher"]["disbursementAmount"] == "50000"
    assert changed_voucher["voucher"]["disbursementDate"] == "2023-09-19T00:00:00"
    assert changed_voucher["voucher"]["disbursementNumber"] == "2384230"
    assert changed_voucher["voucher"]["status"] == "Paid"


@pytest.mark.parametrize("mock_invoice_tasks", ["success"], indirect=True)
@patch("libsys_airflow.plugins.orafin.tasks._folio_client")
def test_update_vouchers_task_failed(
    patched_folio_client, mock_folio_client, mock_invoice_tasks, caplog
):
    voucher = {
        "paid": "Voucher 992a30bc-c65b-4514-8d11-f8d325e2f10a already Paid",
        "voucher": {
            "id": "992a30bc-c65b-4514-8d11-f8d325e2f10a",
            "status": "Paid",
            "disbursementNumber": "Pending",
            "disbursementAmount": 0,
            "invoiceId": "3cf0ebad-6e86-4374-a21d-daf2227b09cd",
        },
    }

    patched_folio_client.return_value = mock_folio_client
    changed_voucher = update_vouchers_task.function(voucher, mock_invoice_tasks)

    assert (
        "Failed to update voucher 992a30bc-c65b-4514-8d11-f8d325e2f10a" in caplog.text
    )
    assert changed_voucher["failed"]["id"] == "992a30bc-c65b-4514-8d11-f8d325e2f10a"


def test_process_folio_results_task():
    update_folio_results = [
        {
            "invoice": {
                "invoice": {
                    "id": "c7941b2e-ef06-406e-af82-863717ff4ba0",
                    "status": "Paid",
                },
                "success": {
                    "id": "c7941b2e-ef06-406e-af82-863717ff4ba0",
                    "status": "Paid",
                },
                "c7941b2e-ef06-406e-af82-863717ff4ba0": {
                    "PoNumber": None,
                    "AmountPaid": "10191",
                    "InvoiceAmt": "10191",
                    "InvoiceNum": "1499 40002",
                    "InvoiceDate": "05/22/2026",
                    "PaymentDate": "06/16/2026",
                    "SupplierName": "VIGILANTE RARE DOCUMENTS",
                    "PaymentAmount": "10191",
                    "PaymentNumber": "2501659",
                    "SupplierNumber": "410302",
                },
            },
            "voucher": {
                "paid": "Voucher 1ad7d48e-4608-48e3-b9e5-51c7499932fb already Paid",
                "success": {
                    "id": "1ad7d48e-4608-48e3-b9e5-51c7499932fb",
                    "status": "Paid",
                    "invoiceId": "c7941b2e-ef06-406e-af82-863717ff4ba0",
                    "disbursementDate": "2026-06-16T00:00:00",
                    "disbursementAmount": "10191",
                    "disbursementNumber": "2501659",
                },
                "voucher": {
                    "id": "1ad7d48e-4608-48e3-b9e5-51c7499932fb",
                    "status": "Paid",
                    "invoiceId": "c7941b2e-ef06-406e-af82-863717ff4ba0",
                    "disbursementDate": "2026-06-16T00:00:00",
                    "disbursementAmount": "10191",
                    "disbursementNumber": "2501659",
                },
            },
        }
    ]
    folio_results_processed = process_folio_results_task.function(update_folio_results)
    assert len(folio_results_processed["already_paid_invoices"]) == 0
    assert folio_results_processed["already_paid_vouchers"] == [
        "Voucher 1ad7d48e-4608-48e3-b9e5-51c7499932fb already Paid"
    ]
    assert len(folio_results_processed["failed_invoice_updates"]) == 0
    assert len(folio_results_processed["failed_voucher_updates"]) == 0
    assert len(folio_results_processed["cancelled_invoices"]) == 0
    assert len(folio_results_processed["missing_invoices"]) == 0
    assert len(folio_results_processed["missing_vouchers"]) == 0
    assert len(folio_results_processed["multiple_vouchers"]) == 0
    assert (
        folio_results_processed["successful_invoice_updates"][0]["id"]
        == "c7941b2e-ef06-406e-af82-863717ff4ba0"
    )
    assert (
        folio_results_processed["successful_voucher_updates"][0]["id"]
        == "1ad7d48e-4608-48e3-b9e5-51c7499932fb"
    )


def test_process_many_folio_results_task():
    update_folio_results = [
        {
            "invoice": [
                {
                    "failed": {
                        "PoNumber": None,
                        "AmountPaid": "913.81",
                        "InvoiceAmt": "913.81",
                        "InvoiceNum": "F260516273 39892",
                        "invoice_id": "48e9c078-ac3a-41a0-9bee-262e785df5f5",
                        "InvoiceDate": "05/04/2026",
                        "PaymentDate": "06/15/2026",
                        "SupplierName": "AUX AMATEURS DE LIVRES",
                        "PaymentAmount": "3036.29",
                        "PaymentNumber": "2501484",
                        "SupplierNumber": "002685",
                    },
                    "invoice": {
                        "id": "48e9c078-ac3a-41a0-9bee-262e785df5f5",
                        "total": 780.1,
                        "source": "EDI",
                        "status": "Paid",
                    },
                    "48e9c078-ac3a-41a0-9bee-262e785df5f5": {
                        "PoNumber": None,
                        "AmountPaid": "913.81",
                        "InvoiceAmt": "913.81",
                        "InvoiceNum": "F260516273 39892",
                        "invoice_id": "48e9c078-ac3a-41a0-9bee-262e785df5f5",
                        "InvoiceDate": "05/04/2026",
                        "PaymentDate": "06/15/2026",
                        "SupplierName": "AUX AMATEURS DE LIVRES",
                        "PaymentAmount": "3036.29",
                        "PaymentNumber": "2501484",
                        "SupplierNumber": "002685",
                    },
                },
                {
                    "missing": {
                        "PoNumber": None,
                        "AmountPaid": "696.27",
                        "InvoiceAmt": "696.27",
                        "InvoiceNum": "F260516457 40046",
                        "InvoiceDate": "05/05/2026",
                        "PaymentDate": "06/15/2026",
                        "SupplierName": "AUX AMATEURS DE LIVRES",
                        "PaymentAmount": "3036.29",
                        "PaymentNumber": "2501484",
                        "SupplierNumber": "002685",
                    }
                },
                {
                    "paid": {
                        "PoNumber": None,
                        "AmountPaid": "3395",
                        "InvoiceAmt": "3395",
                        "InvoiceNum": "I-4129 39771",
                        "invoice_id": "27d8ba0a-9b64-42f2-9c24-5112ec510dae",
                        "InvoiceDate": "05/17/2026",
                        "PaymentDate": "06/16/2026",
                        "SupplierName": "JEFFREY D MANCEVICE RARE BOOKS",
                        "PaymentAmount": "3395",
                        "PaymentNumber": "3915381",
                        "SupplierNumber": "017102",
                    }
                },
                {
                    "cancelled": {
                        "PoNumber": None,
                        "AmountPaid": "24.25",
                        "InvoiceAmt": "24.25",
                        "InvoiceNum": "SO64938 40151",
                        "invoice_id": "37c8ba0a-9f64-42f2-0c68-5112ec510ste",
                        "InvoiceDate": "05/01/2026",
                        "PaymentDate": "06/16/2026",
                        "SupplierName": "KINOKUNIYA BOOK STORES OF AMERICA CO LTD",
                        "PaymentAmount": "398.95",
                        "PaymentNumber": "3915189",
                        "SupplierNumber": "940514",
                    }
                },
                {
                    "success": {
                        "id": "e4be44c1-f98a-4b19-b61d-09fe095c6fa3",
                        "status": "Paid",
                    },
                    "invoice": {
                        "id": "e4be44c1-f98a-4b19-b61d-09fe095c6fa3",
                        "status": "Paid",
                    },
                    "e4be44c1-f98a-4b19-b61d-09fe095c6fa3": {
                        "PoNumber": None,
                        "AmountPaid": "858.07",
                        "InvoiceAmt": "858.07",
                        "InvoiceNum": "368945 37186",
                        "invoice_id": "e4be44c1-f98a-4b19-b61d-09fe095c6fa3",
                        "InvoiceDate": "01/12/2026",
                        "PaymentDate": "06/16/2026",
                        "SupplierName": "OTTO HARRASSOWITZ GMBH AND CO KG",
                        "PaymentAmount": "7102.42",
                        "PaymentNumber": "3915319",
                        "SupplierNumber": "012580",
                    },
                },
                {
                    "invoice": {
                        "id": "c7941b2e-ef06-406e-af82-863717ff4ba0",
                        "status": "Paid",
                    },
                    "success": {
                        "id": "c7941b2e-ef06-406e-af82-863717ff4ba0",
                        "status": "Paid",
                    },
                    "c7941b2e-ef06-406e-af82-863717ff4ba0": {
                        "PoNumber": None,
                        "AmountPaid": "10191",
                        "InvoiceAmt": "10191",
                        "InvoiceNum": "1499 40002",
                        "InvoiceDate": "05/22/2026",
                        "PaymentDate": "06/16/2026",
                        "SupplierName": "VIGILANTE RARE DOCUMENTS",
                        "PaymentAmount": "10191",
                        "PaymentNumber": "2501659",
                        "SupplierNumber": "410302",
                    },
                },
            ],
            "voucher": [
                None,
                None,
                None,
                None,
                {
                    "paid": "Voucher 1ad7d48e-4608-48e3-b9e5-51c7499932fb already Paid",
                    "success": {
                        "id": "1ad7d48e-4608-48e3-b9e5-51c7499932fb",
                        "status": "Paid",
                        "invoiceId": "c7941b2e-ef06-406e-af82-863717ff4ba0",
                        "disbursementDate": "2026-06-16T00:00:00",
                        "disbursementAmount": "10191",
                        "disbursementNumber": "2501659",
                    },
                    "voucher": {
                        "id": "1ad7d48e-4608-48e3-b9e5-51c7499932fb",
                        "status": "Paid",
                        "invoiceId": "c7941b2e-ef06-406e-af82-863717ff4ba0",
                        "disbursementDate": "2026-06-16T00:00:00",
                        "disbursementAmount": "10191",
                        "disbursementNumber": "2501659",
                    },
                },
                {
                    "paid": "Voucher 2ad7d48e-4608-48e3-b9e5-51c7499933gn already Paid",
                    "voucher": {
                        "id": "2ad7d48e-4608-48e3-b9e5-51c7499933gn",
                        "status": "Paid",
                        "invoiceId": "e4be44c1-f98a-4b19-b61d-09fe095c6fa3",
                        "disbursementDate": "2026-06-16T00:00:00",
                        "disbursementAmount": "10191",
                        "disbursementNumber": "2501659",
                    },
                    "failed": {
                        "id": "2ad7d48e-4608-48e3-b9e5-51c7499933gn",
                        "status": "Paid",
                        "invoiceId": "e4be44c1-f98a-4b19-b61d-09fe095c6fa3",
                        "disbursementDate": "2026-06-16T00:00:00",
                        "disbursementAmount": "10191",
                        "disbursementNumber": "2501659",
                    },
                },
                {"missing": "3379cf1d-dd47-4f7f-9b04-7ace791e75c8"},
                {"multiple": "e2e8344d-2ad6-44f2-bf56-f3cd04f241b3"},
            ],
        }
    ]
    folio_results_processed = process_folio_results_task.function(update_folio_results)
    assert (
        folio_results_processed["already_paid_invoices"][0]["invoice_id"]
        == "27d8ba0a-9b64-42f2-9c24-5112ec510dae"
    )
    assert (
        folio_results_processed["already_paid_vouchers"][0]
        == "Voucher 1ad7d48e-4608-48e3-b9e5-51c7499932fb already Paid"
    )
    assert (
        folio_results_processed["failed_invoice_updates"][0]["invoice_id"]
        == "48e9c078-ac3a-41a0-9bee-262e785df5f5"
    )
    assert (
        folio_results_processed["failed_voucher_updates"][0]["invoiceId"]
        == "e4be44c1-f98a-4b19-b61d-09fe095c6fa3"
    )
    assert (
        folio_results_processed["failed_voucher_updates"][0]["id"]
        == "2ad7d48e-4608-48e3-b9e5-51c7499933gn"
    )
    assert (
        folio_results_processed["cancelled_invoices"][0]["invoice_id"]
        == "37c8ba0a-9f64-42f2-0c68-5112ec510ste"
    )
    assert (
        folio_results_processed["missing_invoices"][0]["InvoiceNum"]
        == "F260516457 40046"
    )
    assert (
        folio_results_processed["missing_vouchers"][0]
        == "3379cf1d-dd47-4f7f-9b04-7ace791e75c8"
    )
    assert (
        folio_results_processed["multiple_vouchers"][0]
        == "e2e8344d-2ad6-44f2-bf56-f3cd04f241b3"
    )
    assert (
        folio_results_processed["successful_invoice_updates"][0]["id"]
        == "e4be44c1-f98a-4b19-b61d-09fe095c6fa3"
    )
    assert (
        folio_results_processed["successful_invoice_updates"][1]["id"]
        == "c7941b2e-ef06-406e-af82-863717ff4ba0"
    )
    assert (
        folio_results_processed["successful_voucher_updates"][0]["id"]
        == "1ad7d48e-4608-48e3-b9e5-51c7499932fb"
    )


def test_email_invoice_errors_task(mocker, caplog):
    missing = [{"InvoiceNum": "F260516457 40046"}]
    cancelled = [
        {
            "InvoiceNum": "SO64938 40151",
            "invoice_id": "37c8ba0a-9f64-42f2-0c68-5112ec510ste",
        }
    ]
    already_paid = [
        {
            "InvoiceNum": "I-4129 39771",
            "invoice_id": "27d8ba0a-9b64-42f2-9c24-5112ec510dae",
        }
    ]
    failed_updates = [
        {
            "InvoiceNum": "F260516273 39892",
            "invoice_id": "48e9c078-ac3a-41a0-9bee-262e785df5f5",
        }
    ]
    mocker.patch(
        "libsys_airflow.plugins.orafin.tasks.generate_ap_error_report_email",
        return_value=True,
    )
    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )
    mocker.patch("libsys_airflow.plugins.orafin.emails.send_email_with_server_name")
    email_task = email_invoice_errors_task.function(
        missing, cancelled, already_paid, failed_updates
    )
    assert email_task is True
    assert "Emailing 4 error reports" in caplog.text


def test_failed_email_invoice_errors_task(mocker, caplog):
    missing = []
    cancelled = []
    already_paid = []
    failed_updates = [
        {
            "InvoiceNum": "F260516273 39892",
            "invoice_id": "48e9c078-ac3a-41a0-9bee-262e785df5f5",
        }
    ]
    mocker.patch(
        "libsys_airflow.plugins.orafin.tasks.generate_ap_error_report_email",
        return_value=False,
    )
    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )
    with pytest.raises(AirflowException) as exc_info:
        email_invoice_errors_task.function(
            missing, cancelled, already_paid, failed_updates
        )
    assert "Failed to send error report emails." in str(exc_info.value)
    assert "Emailing 1 error reports" in caplog.text


def test_skip_email_invoice_errors_task():
    missing = []
    cancelled = []
    already_paid = []
    failed_updates = []
    with pytest.raises(AirflowSkipException) as exc_info:
        email_invoice_errors_task.function(
            missing, cancelled, already_paid, failed_updates
        )
    assert "No invoice errors to report." in str(exc_info.value)


def test_email_vouchers_errors_task(mocker, caplog):
    missing = ["3379cf1d-dd47-4f7f-9b04-7ace791e75c8"]
    multiple = ["3379cf1d-dd47-4f7f-9b04-7ace791e75c8"]
    failed_updates = [
        {
            "id": "2ad7d48e-4608-48e3-b9e5-51c7499933gn",
            "status": "Paid",
            "invoiceId": "e4be44c1-f98a-4b19-b61d-09fe095c6fa3",
        }
    ]
    mocker.patch(
        "libsys_airflow.plugins.orafin.tasks.generate_voucher_error_email",
        return_value=True,
    )
    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )
    mocker.patch("libsys_airflow.plugins.orafin.emails.send_email_with_server_name")
    email_task = email_vouchers_errors_task.function(missing, multiple, failed_updates)
    assert email_task is True
    assert "Emailing 3 error reports" in caplog.text


def test_failed_email_vouchers_errors_task(mocker, caplog):
    missing = ["3379cf1d-dd47-4f7f-9b04-7ace791e75c8"]
    multiple = ["3379cf1d-dd47-4f7f-9b04-7ace791e75c8"]
    failed_updates = []
    mocker.patch(
        "libsys_airflow.plugins.orafin.tasks.generate_voucher_error_email",
        return_value=False,
    )
    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )
    with pytest.raises(AirflowException) as exc_info:
        email_vouchers_errors_task.function(missing, multiple, failed_updates)
    assert "Failed to send voucher error report emails." in str(exc_info.value)
    assert "Emailing 2 error reports" in caplog.text


def test_skip_email_vouchers_errors_task():
    missing = []
    multiple = []
    failed_updates = []
    with pytest.raises(AirflowSkipException) as exc_info:
        email_vouchers_errors_task.function(missing, multiple, failed_updates)
    assert "No voucher errors to report." in str(exc_info.value)


def test_email_paid_task(mocker, caplog):
    paid = [
        {"id": "e4be44c1-f98a-4b19-b61d-09fe095c6fa3", "status": "Paid"},
        {"id": "c7941b2e-ef06-406e-af82-863717ff4ba0", "status": "Paid"},
    ]
    report_path = "/opt/airflow/orafin-files/reports/xxdl_ap_payment_06162026153000.csv"
    mocker.patch(
        "libsys_airflow.plugins.orafin.tasks.generate_ap_paid_report_email",
        return_value={"sul": True, "bus": False},
    )
    mocker.patch(
        "libsys_airflow.plugins.orafin.emails.Variable.get",
        return_value="test@stanford.edu",
    )
    mocker.patch("libsys_airflow.plugins.orafin.emails.send_email_with_server_name")
    email_paid_task.function(paid, report_path)
    assert "Emailing all 2 paid invoices" in caplog.text
    assert "Failed to send paid email report to bus" in caplog.text
    assert "Sent paid email report to sul" in caplog.text


def test_skip_email_paid_task():
    paid = []
    report_path = "/opt/airflow/orafin-files/reports/xxdl_ap_payment_06162026153000.csv"
    with pytest.raises(AirflowSkipException) as exc_info:
        email_paid_task.function(paid, report_path)
    assert "No paid invoices to report." in str(exc_info)
