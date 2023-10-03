import datetime
import logging
import pathlib
import shlex

import numpy as np
import pandas as pd

from typing import Union
from airflow.operators.python import get_current_context

from libsys_airflow.plugins.folio.folio_client import FolioClient

logger = logging.getLogger(__name__)


def _retrieve_invoice(
    invoice_number: str, folio_client: FolioClient, task_instance
) -> Union[dict, None]:
    """
    Takes InvoiceNum, separates into vendorInvoiceNo and folioInvoiceNo
    values to retrieve invoice
    """
    parts = shlex.split(invoice_number)
    _, folio_invoice_number = parts[0], parts[1]
    invoice_result = folio_client.get(
        f"""/invoice/invoices?query=(folioInvoiceNo == "{folio_invoice_number}")""",
    )
    match len(invoice_result["invoices"]):
        case 0:
            msg = f"No Invoice found for folioInvoiceNo {folio_invoice_number}"
            logger.error(msg)
            task_instance.xcom_push(key="missing", value=msg)

        case 1:
            invoice = invoice_result["invoices"][0]
            match invoice["status"]:

                case "Cancelled":
                    msg = f"Invoice {invoice['id']} has been Cancelled"
                    logger.error(msg)
                    task_instance.xcom_push(key="cancelled", value=invoice['id'])

                case "Paid":
                    msg = f"Invoice {invoice['id']} already Paid"
                    logger.error(msg)
                    task_instance.xcom_push(key="paid", value=invoice['id'])
            
                case _:
                    return invoice

        case _:
            msg = f"Multiple invoices {','.join([invoice['id'] for invoice in invoice_result['invoices']])} found for folioInvoiceNo {folio_invoice_number}"
            logger.error(msg)
            task_instance.xcom_push(key="duplicates", value=msg)


def email_reporting_errors(folio_url: str) -> int:
    """
    Retrieves Errors from upstream tasks and emails report
    """
    task_instance = get_current_context()["ti"]
    logger.info("Generating Email Report")
    missing_invoices = task_instance.xcom_pull(task_ids='retrieve_invoice_task', key='missing')
    if missing_invoices is None:
        missing_invoices = []
    logger.info(f"Missing {len(missing_invoices):,}")
    cancelled_invoices = task_instance.xcom_pull(task_ids='retrieve_invoice_task', key='cancelled')
    if cancelled_invoices is None:
        cancelled_invoices = []
    logger.info(f"Cancelled {len(cancelled_invoices):,}")
    paid_invoices = task_instance.xcom_pull(task_ids='retrieve_invoice_task', key='paid')
    if paid_invoices is None:
        paid_invoices = []
    logger.info(f"Paid {len(paid_invoices):,}")
    
    return len(missing_invoices) + len(cancelled_invoices) + len(paid_invoices)


def retrieve_voucher(invoice_id: str, folio_client: FolioClient) -> Union[dict, None]:
    """
    Retrieves voucher based on the invoice id
    """
    voucher_result = folio_client.get(
        f"/voucher-storage/vouchers?query=(invoiceId=={invoice_id})"
    )
    task_instance = get_current_context()["ti"]
    match len(voucher_result["vouchers"]):
        case 0:
            msg = f"No voucher found for invoice {invoice_id}"
            logger.error(msg)
            task_instance.xcom_push(key="missing", value=msg)

        case 1:
            voucher = voucher_result["vouchers"][0]
            if voucher["status"] == "Paid":
                msg = f"Voucher {voucher['id']} already Paid"
                logger.error(msg)
                task_instance.xcom_push(key="paid", value=msg)
            else:
                return voucher

        case _:
            msg = f"Multiple vouchers {','.join([voucher['id'] for voucher in voucher_result['vouchers']])} found for invoice {invoice_id}"
            logger.error(msg)
            task_instance.xcom_push(key="duplicates", value=msg)


def update_invoice(invoice: dict):
    """
    Updates Invoice
    """
    invoice["status"] = "Paid"
    logger.info(f"Need to PUT {invoice['id']} to FOLIO")


def update_voucher(voucher: dict) -> dict:
    """
    Updates Voucher based on row values
    """
    task_instance = get_current_context()["ti"]
    row = task_instance.xcom_pull(
        task_ids="retrieve_invoice_task", key=voucher["invoiceId"]
    )[0]
    voucher["amount"] = row["PaymentAmount"]
    voucher["disbursementNumber"] = row["PaymentNumber"]
    disbursement_date = datetime.datetime.strptime(row["PaymentDate"], "%m/%d/%Y")
    voucher["disbursementDate"] = disbursement_date.isoformat()

    logger.info(f"Need to PUT {voucher['id']} to FOLIO\n{voucher}")


def retrieve_invoice(row: dict, folio_client: FolioClient) -> dict:
    task_instance = get_current_context()["ti"]
    invoice = _retrieve_invoice(row["InvoiceNum"], folio_client, task_instance)
    if invoice:
        task_instance.xcom_push(key=invoice["id"], value=row)
    return invoice


def retrieve_rows(retrieved_csv: str, airflow: str = "/opt/airflow") -> list:
    """
    Process AP csv file and returns a dictionary of updated
    """
    report_path = pathlib.Path(airflow) / f"orafin-files/reports/{retrieved_csv}"
    with report_path.open() as fo:
        raw_report = fo.readlines()
    field_names = [name.strip() for name in raw_report[0].split(",")]
    report = []
    for row in raw_report[1:]:
        fields = [field.strip() for field in row.split(",")]
        if len(fields) > len(field_names):
            # Combines Supplier Names because name has a comma
            supplier_name = ", ".join([fields[1], fields.pop(2)])
            fields[1] = supplier_name
        report_line = {}
        for name, value in zip(field_names, fields):
            report_line[name] = value
        report.append(report_line)
    report_df = pd.DataFrame(report)
    return report_df.replace({np.nan: None}).to_dict(orient='records')
