import logging
import pathlib
import shlex

import httpx
import numpy as np
import pandas as pd

from datetime import datetime
from typing import Union

from airflow.models.mappedoperator import OperatorPartial
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from folioclient import FolioClient

from libsys_airflow.plugins.shared.utils import is_production

logger = logging.getLogger(__name__)

ap_server_options = [
    "-i /opt/airflow/vendor-keys/apserver.key",
    "-o StrictHostKeyChecking=no",
]


def get_orafin_server_info() -> str:
    if is_production():
        return "of_aplib@intxfer-prd.stanford.edu"
    return "of_aplib@intxfer-uat.stanford.edu"


def _retrieve_invoice(
    report_row: dict, folio_client: FolioClient, task_instance
) -> Union[dict, None]:
    """
    Takes InvoiceNum and parses out the folioInvoiceNo values to retrieve invoice from
    Okapi
    """
    invoice_number = report_row["InvoiceNum"]
    parts = shlex.split(invoice_number)
    _, folio_invoice_number = parts[0], parts[1]
    invoices = folio_client.folio_get(
        f"""/invoice/invoices?query=(folioInvoiceNo == "{folio_invoice_number}")""",
        key="invoices",
    )
    match len(invoices):
        case 0:
            msg = f"No Invoice found for folioInvoiceNo {folio_invoice_number}"
            logger.error(msg)
            task_instance.xcom_push(key="missing", value=report_row)

        case 1:
            invoice = invoices[0]
            match invoice["status"]:
                case "Cancelled":
                    msg = f"Invoice {invoice['id']} has been Cancelled"
                    logger.error(msg)
                    report_row['invoice_id'] = invoice['id']
                    task_instance.xcom_push(key="cancelled", value=report_row)

                case "Paid":
                    msg = f"Invoice {invoice['id']} already Paid"
                    logger.error(msg)
                    report_row['invoice_id'] = invoice['id']
                    task_instance.xcom_push(key="paid", value=report_row)

                case _:
                    return invoice

        case _:
            invoice_ids = [invoice['id'] for invoice in invoices]
            msg = f"Multiple invoices {','.join(invoice_ids)} found for folioInvoiceNo {folio_invoice_number}"
            logger.error(msg)
            report_row['invoice_ids'] = invoice_ids
            task_instance.xcom_push(key="duplicates", value=report_row)
    return None


def extract_rows(retrieved_csv: str) -> tuple:
    """
    Process AP csv file and returns a dictionary of updated
    """
    report_path = pathlib.Path(retrieved_csv)
    dag_run_operator = None
    with report_path.open() as fo:
        raw_report = fo.readlines()
    if len(raw_report) == 1:
        # Blank report, delete and return empty list
        report_path.unlink()
        return [], dag_run_operator
    report_df = pd.read_csv(report_path, sep="\t", dtype="object")
    if len(report_df) > 1_000:
        remaining_df = report_df.iloc[1_000:]
        report_df = report_df.iloc[0:1_000]
        remaining_path = report_path.parent / f"{report_path.stem}_01.csv"
        remaining_df.to_csv(remaining_path, sep="\t")
        dag_run_operator = TriggerDagRunOperator(
            task_id="additional-rows",
            trigger_dag_id="ap_payment_report",
            conf={"ap_report_path": str(remaining_path.absolute())},
        )
    report_rows = report_df.replace({np.nan: None}).to_dict(orient='records')
    return report_rows, dag_run_operator


def filter_files(ls_output, airflow="/opt/airflow") -> tuple:
    """
    Filters files based if they already exist in the orafin-data directory
    """
    reports = [row.strip() for row in ls_output.split(",") if row.endswith(".csv")]
    existing_reports, new_reports = [], []
    for report in reports:
        report_path = pathlib.Path(airflow) / f"orafin-files/reports/{report}"
        if report_path.exists():
            existing_reports.append({"file_name": report_path.name})
        else:
            new_reports.append({"file_name": report_path.name})
    return existing_reports, new_reports


def find_reports() -> BashOperator:
    """
    Looks for reports using ssh with the BashOperator
    """
    command = (
        ["ssh"]
        + ap_server_options
        + [
            f"{get_orafin_server_info()} "
            "ls -m /home/of_aplib/OF1_PRD/outbound/data/*.csv | tr '\n' ' '"
        ]
    )
    return BashOperator(
        task_id="find_files", bash_command=" ".join(command), do_xcom_push=True
    )


def remove_reports() -> OperatorPartial:
    """
    Removes all ap reports from the server
    """
    command = (
        ["ssh"]
        + ap_server_options
        + [
            get_orafin_server_info(),
            "rm /home/of_aplib/OF1_PRD/outbound/data/$file_name",
        ]
    )
    return BashOperator.partial(
        task_id="remove_files", bash_command=" ".join(command), do_xcom_push=True
    )


def retrieve_invoice(row: dict, folio_client: FolioClient) -> Union[None, dict]:
    task_instance = get_current_context()["ti"]
    invoice = _retrieve_invoice(row, folio_client, task_instance)
    if invoice:
        task_instance.xcom_push(key=invoice["id"], value=row)
        return invoice
    return None


def retrieve_voucher(invoice_id: str, folio_client: FolioClient) -> Union[dict, None]:
    """
    Retrieves voucher based on the invoice id
    """
    vouchers = folio_client.folio_get(
        f"/voucher-storage/vouchers?query=(invoiceId=={invoice_id})", key="vouchers"
    )
    task_instance = get_current_context()["ti"]
    match len(vouchers):
        case 0:
            msg = f"No voucher found for invoice {invoice_id}"
            logger.error(msg)
            task_instance.xcom_push(key="missing", value=msg)

        case 1:
            voucher = vouchers[0]
            # Invoice BL endpoint sets voucher status but still needs
            # additional data set from AP report
            if voucher["status"] == "Paid":
                msg = f"Voucher {voucher['id']} already Paid"
                logger.error(msg)
                task_instance.xcom_push(key="paid", value=msg)
            return voucher

        case _:
            msg = f"Multiple vouchers {','.join([voucher['id'] for voucher in vouchers])} found for invoice {invoice_id}"
            logger.error(msg)
            task_instance.xcom_push(key="duplicates", value=msg)
    return None


def retrieve_reports() -> OperatorPartial:
    """
    scp AP Reports from server
    """
    server_info = get_orafin_server_info()
    file_location = "/home/of_aplib/OF1_DEV/outbound/data/$file_name"
    if is_production():
        file_location = "/home/of_aplib/OF1_PRD/outbound/data/$file_name"
    command = (
        ["scp"]
        + ap_server_options
        + [
            f"{server_info}:{file_location}",
            "/opt/airflow/orafin-files/reports/",
        ]
    )
    return BashOperator.partial(task_id="scp_report", bash_command=" ".join(command))


def update_invoice(invoice: dict, folio_client: FolioClient) -> Union[dict, bool]:
    """
    Updates Invoice
    """
    invoice["status"] = "Paid"
    try:
        folio_client.folio_put(f"/invoice/invoices/{invoice['id']}", invoice)
        logger.info(f"Updated {invoice['id']} to status of Paid")
    except httpx.HTTPError:
        return False
    return invoice


def update_voucher(
    voucher: dict, task_instance, folio_client: FolioClient
) -> Union[dict, bool]:
    """
    Updates Voucher based on row values
    """
    row = task_instance.xcom_pull(
        task_ids="retrieve_invoice_task", key=voucher["invoiceId"]
    )[0]
    voucher["status"] = "Paid"  # should already be Paid when update_invoice task ran
    voucher["disbursementAmount"] = row["AmountPaid"]
    voucher["disbursementNumber"] = row["PaymentNumber"]
    disbursement_date = datetime.strptime(row["PaymentDate"], "%m/%d/%Y")
    voucher["disbursementDate"] = disbursement_date.isoformat()
    try:
        folio_client.folio_put(f"/voucher/vouchers/{voucher['id']}", voucher)
        logger.info(f"Updated {voucher['id']}")
    except httpx.HTTPError:
        logger.warning(f"Failed to update voucher {voucher['id']}")
        return False

    return voucher
