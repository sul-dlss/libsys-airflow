import logging
import pathlib
import shlex
import httpx
from datetime import datetime
from typing import Union

from airflow.sdk import get_current_context, task, Variable
from airflow.sdk.exceptions import AirflowSkipException, AirflowException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from folioclient import FolioClient

from libsys_airflow.plugins.orafin.emails import (
    generate_ap_error_report_email,
    generate_ap_paid_report_email,
    generate_excluded_email,
    generate_voucher_error_email,
    generate_summary_email,
)

from libsys_airflow.plugins.orafin.reports import (
    extract_rows,
    filter_files,
)

from libsys_airflow.plugins.orafin.payments import (
    generate_file,
    get_invoice,
    init_feeder_file,
    models_converter,
    transfer_to_orafin,
)


logger = logging.getLogger(__name__)


def _folio_client():
    try:
        return FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )
    except ValueError as error:
        logger.error(error)
        raise


@task
def consolidate_reports_task(ti=None):
    existing_reports = ti.xcom_pull(
        task_ids="filter_files_task", key="existing_reports"
    )
    new_reports = ti.xcom_pull(task_ids="filter_files_task", key="new_reports")
    all_files = []
    for row in existing_reports + new_reports:
        all_files.append(row)
    logger.info(f"Consolidated {all_files}")
    return all_files


@task(trigger_rule="none_failed")
def email_invoice_errors_task(missing, cancelled, already_paid, failed_updates):
    if any([missing, cancelled, already_paid, failed_updates]):
        total_errors = (
            len(missing) + len(cancelled) + len(already_paid) + len(failed_updates)
        )
        logger.info(f"Emailing {total_errors:,} error reports")
        email_sent = generate_ap_error_report_email(
            missing, cancelled, already_paid, failed_updates
        )
        if email_sent is False:
            raise AirflowException("Failed to send error report emails.")
        return email_sent
    else:
        raise AirflowSkipException("No invoice errors to report.")


@task(trigger_rule="none_failed")
def email_vouchers_errors_task(missing, multiple, failed_updates):
    if any([missing, multiple, failed_updates]):
        total_errors = len(missing) + len(multiple) + len(failed_updates)
        logger.info(f"Emailing {total_errors:,} error reports")
        email_sent = generate_voucher_error_email(missing, multiple, failed_updates)
        if email_sent is False:
            raise AirflowException("Failed to send voucher error report emails.")
        return email_sent
    else:
        raise AirflowSkipException("No voucher errors to report.")


@task(trigger_rule="none_failed")
def email_paid_task(paid, report_path):
    if len(paid) > 0:
        logger.info(f"Emailing all {len(paid)} paid invoices")
        email_sent = generate_ap_paid_report_email(paid, report_path)
        for lib, result in email_sent.items():
            if result is False:
                logger.error(f"Failed to send paid email report to {lib}")
            else:
                logger.info(f"Sent paid email report to {lib}")
    else:
        raise AirflowSkipException("No paid invoices to report.")


@task
def email_excluded_task(invoices_exclusion_reasons: list):
    if len(invoices_exclusion_reasons) > 0:
        generate_excluded_email(invoices_exclusion_reasons)
    return f"Emailed report for {len(invoices_exclusion_reasons):,} invoices"


@task
def email_summary_task(invoices: list):
    generate_summary_email(invoices)
    return f"Emailed summary report for {len(invoices):,} invoices"


def _group_updates(data: dict) -> tuple:
    successes: list = []
    failed: list = []
    missing: list = []
    cancelled: list = []
    paid: list = []
    multiple: list = []
    invoice_or_voucher: list = []
    for key, value in data.items():
        match key:
            case "success":
                successes.append(value)
            case "failed":
                failed.append(value)
            case "missing":
                missing.append(value)
            case "cancelled":
                cancelled.append(value)
            case "paid":
                paid.append(value)
            case "multiple":
                multiple.append(value)
            case "invoice":
                pass
            case _:
                invoice_or_voucher.append({key: value})
    return (successes, failed, missing, cancelled, paid, multiple, invoice_or_voucher)


@task(trigger_rule="none_failed")
def process_folio_results_task(update_folio_results) -> dict:
    """
    update_folio_results is a list of dicts if one mapped task,
    otherwise it is a list of list of dicts
    invoice should always be a dict; voucher could be None or dict
    update_folio_results = [{"invoice": [update_invoice_result], "voucher": [update_voucher_result]},
                            {"invoice":..., "voucher":...}]
    update_invoice_result = {"<invoice_uuid>": {ap_payment_report_row},
                              "invoice": {folio_invoice_data},
                              "missing": {ap_payment_report_row},
                              "cancelled": {ap_payment_report_row},
                              "paid": {ap_payment_report_row},
                              "success": {folio_invoice_data},
                              "failed": {ap_payment_report_row}
    update_voucher_result = {"missing": "", "paid": "", "multiple": "", "voucher": {folio_voucher_data},
                              "success|failed": {voucher_to_update}}
    """
    successful_invoice_updates: list = []
    failed_invoice_updates: list = []
    missing_invoices: list = []
    cancelled_invoices: list = []
    already_paid_invoices: list = []
    invoice_id_ap_rpt: list = []
    successful_voucher_updates: list = []
    failed_voucher_updates: list = []
    missing_vouchers: list = []
    already_paid_vouchers: list = []
    multiple_vouchers: list = []
    vouchers: list = []

    for result in update_folio_results:
        invoice_results = result.get("invoice")
        voucher_results = result.get("voucher")
        if isinstance(invoice_results, dict):
            invoice = result.get("invoice")
            (
                successes,
                failed,
                missing,
                cancelled,
                paid,
                multiple,
                invoice_or_voucher,
            ) = _group_updates(invoice)
            successful_invoice_updates.extend(successes)
            failed_invoice_updates.extend(failed)
            missing_invoices.extend(missing)
            cancelled_invoices.extend(cancelled)
            already_paid_invoices.extend(paid)
            invoice_id_ap_rpt.extend(invoice_or_voucher)
        if isinstance(invoice_results, list):
            for invoice in result.get("invoice"):
                (
                    successes,
                    failed,
                    missing,
                    cancelled,
                    paid,
                    multiple,
                    invoice_or_voucher,
                ) = _group_updates(invoice)
                successful_invoice_updates.extend(successes)
                failed_invoice_updates.extend(failed)
                missing_invoices.extend(missing)
                cancelled_invoices.extend(cancelled)
                already_paid_invoices.extend(paid)
                invoice_id_ap_rpt.extend(invoice_or_voucher)
        if isinstance(voucher_results, dict):
            voucher = result.get("voucher")
            if voucher is None:
                pass
            else:
                (
                    successes,
                    failed,
                    missing,
                    cancelled,
                    paid,
                    multiple,
                    invoice_or_voucher,
                ) = _group_updates(voucher)
                successful_voucher_updates.extend(successes)
                failed_voucher_updates.extend(failed)
                missing_vouchers.extend(missing)
                already_paid_vouchers.extend(paid)
                multiple_vouchers.extend(multiple)
                vouchers.extend(invoice_or_voucher)
        if isinstance(voucher_results, list):
            for voucher in result.get("voucher"):
                if voucher is None:
                    pass
                else:
                    (
                        successes,
                        failed,
                        missing,
                        cancelled,
                        paid,
                        multiple,
                        invoice_or_voucher,
                    ) = _group_updates(voucher)
                    successful_voucher_updates.extend(successes)
                    failed_voucher_updates.extend(failed)
                    missing_vouchers.extend(missing)
                    already_paid_vouchers.extend(paid)
                    multiple_vouchers.extend(multiple)
                    vouchers.extend(invoice_or_voucher)
    return {
        "already_paid_invoices": already_paid_invoices,
        "already_paid_vouchers": already_paid_vouchers,
        "failed_invoice_updates": failed_invoice_updates,
        "failed_voucher_updates": failed_voucher_updates,
        "cancelled_invoices": cancelled_invoices,
        "missing_invoices": missing_invoices,
        "missing_vouchers": missing_vouchers,
        "multiple_vouchers": multiple_vouchers,
        "successful_invoice_updates": successful_invoice_updates,
        "successful_voucher_updates": successful_voucher_updates,
    }


@task
def extract_rows_task(**kwargs):
    report_path = kwargs["report_path"]
    report_rows, dag_trigger = extract_rows(report_path)
    if dag_trigger:
        dag_trigger.execute(kwargs)
    return report_rows


@task
def feeder_file_task(invoices: list):
    converter = models_converter()
    folio_client = _folio_client()
    feeder_file = init_feeder_file(invoices, folio_client, converter)

    return converter.unstructure(feeder_file)


@task
def filter_files_task(ti=None):
    ls_output = ti.xcom_pull(task_ids="find_files")
    existing_reports, new_reports = filter_files(ls_output)
    ti.xcom_push(key="existing_reports", value=existing_reports)
    ti.xcom_push(key="new_reports", value=new_reports)


@task(multiple_outputs=True)
def filter_invoices_task(invoices: list):
    feeder_file, excluded = [], []
    for row in invoices:
        if row['exclude'] is True:
            excluded.append(
                {"invoice": row["invoice"], "reason": row["exclusion_reason"]}
            )
        else:
            feeder_file.append(row['invoice'])
    return {"feed": feeder_file, "excluded": excluded}


@task
def generate_feeder_file_task(feeder_file: dict, airflow: str = "/opt/airflow") -> str:
    # Initialize Feeder File Task
    folio_client = _folio_client()
    orafin_path = pathlib.Path(f"{airflow}/orafin-files/data")
    orafin_path.mkdir(exist_ok=True, parents=True)
    feeder_file_instance = generate_file(feeder_file, folio_client)
    feeder_file_path = orafin_path / feeder_file_instance.file_name
    with feeder_file_path.open("w+") as fo:
        fo.write(feeder_file_instance.generate())
    logger.info(f"Feeder-file {feeder_file_path.resolve()}")
    return str(feeder_file_path.resolve())


@task
def get_new_reports_task(ti=None):
    return ti.xcom_pull(task_ids="filter_files_task", key="new_reports")


@task
def init_processing_task():
    context = get_current_context()
    params = context.get("params")
    return params.get("ap_report_path")


@task
def launch_report_processing_task(**kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    ti = kwargs.get("ti")
    new_reports = ti.xcom_pull(task_ids="filter_files_task", key="new_reports")
    for i, report in enumerate(new_reports):
        TriggerDagRunOperator(
            task_id=f"ap_payment_report-{i}",
            trigger_dag_id="ap_payment_report",
            conf={
                "ap_report_path": f"{airflow}/orafin-files/reports/{report['file_name']}"
            },
        ).execute(kwargs)


@task(max_active_tis_per_dagrun=5)
def retrieve_invoice_task(row: dict) -> dict:
    """
    Retrieves invoice from a row dictionary
    Takes InvoiceNum and parses out the folioInvoiceNo values to retrieve invoice from Folio
    Adds invoice_id to ap_payment_report_row for cancelled and already paid invoices
    Returns a dictionary with the following possible items:
    { "<invoice_uuid>": {ap_payment_report_row},
      "invoice": {folio_invoice_data},
      "missing": {ap_payment_report_row},
      "cancelled": {ap_payment_report_row},
      "paid": {ap_payment_report_row}
    }
    """
    folio_client = _folio_client()
    retrieve_invoices_result: dict = {}
    invoice_number = row["InvoiceNum"]
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
            retrieve_invoices_result["missing"] = row
        case _:
            invoice = invoices[0]
            match invoice["status"]:
                case "Cancelled":
                    msg = f"Invoice {invoice['id']} has been Cancelled"
                    logger.error(msg)
                    row["invoice_id"] = invoice["id"]
                    retrieve_invoices_result["cancelled"] = row
                case "Paid":
                    msg = f"Invoice {invoice['id']} already Paid"
                    logger.error(msg)
                    row["invoice_id"] = invoice["id"]
                    retrieve_invoices_result["paid"] = row
                case _:
                    retrieve_invoices_result["invoice"] = invoice

    invoice = retrieve_invoices_result.get("invoice")
    if invoice is not None:
        retrieve_invoices_result[invoice["id"]] = row

    return retrieve_invoices_result


@task
def retrieve_voucher_task(update_result) -> Union[dict, None]:
    """
    Retrieves voucher based on invoice id
    Gets update_invoice_task output:
    { "<invoice_uuid>": {ap_payment_report_row},
      "invoice": {folio_invoice_data},
      "missing": {ap_payment_report_row},
      "cancelled": {ap_payment_report_row},
      "paid": {ap_payment_report_row},
      "success": {folio_invoice_data},
      "failed": {ap_payment_report_row}
    }
    Returns a dictionary: {"missing": "invoice_id", "paid": "", "multiple": "invoice_id", "voucher": {<folio_voucher_data>}}
    """
    if update_result.get("success"):
        invoice_id = update_result["success"]["id"]
        folio_client = _folio_client()
        retrieve_voucher_result: dict = {}
        vouchers = folio_client.folio_get(
            f"/voucher-storage/vouchers?query=(invoiceId=={invoice_id})", key="vouchers"
        )
        match len(vouchers):
            case 0:
                msg = f"No voucher found for invoice {invoice_id}"
                logger.error(msg)
                retrieve_voucher_result["missing"] = invoice_id
            case 1:
                voucher = vouchers[0]
                # Invoice BL endpoint sets voucher status but still needs
                # additional data set from AP report
                if voucher["status"] == "Paid":
                    msg = f"Voucher {voucher['id']} already Paid"
                    logger.info(msg)
                    retrieve_voucher_result["paid"] = msg
                    retrieve_voucher_result["voucher"] = voucher
                else:
                    retrieve_voucher_result["voucher"] = voucher
            case _:
                msg = f"Multiple vouchers {','.join([voucher['id'] for voucher in vouchers])} found for invoice {invoice_id}"
                logger.error(msg)
                retrieve_voucher_result["multiple"] = invoice_id
        return retrieve_voucher_result
    else:
        raise AirflowSkipException(
            "Skipping voucher retrieval - invoice meets error conditions"
        )


# @task -- When SFTP is available on AP server, uncomment this line to make a taskflow task
def sftp_file_task(feeder_file_path: str, sftp_connection: str = None):  # type: ignore
    bash_operator = transfer_to_orafin(feeder_file_path)
    return bash_operator


@task(max_active_tis_per_dag=5)
def transform_folio_data_task(invoice_id: str):
    """
    Takes Invoice ID and retrieves invoice information and tax status
    from the invoice's organization
    """
    folio_client = _folio_client()
    converter = models_converter()
    # Call to Okapi invoice endpoint
    invoice, exclude, reason = get_invoice(invoice_id, folio_client, converter)
    return {
        "invoice": converter.unstructure(invoice),
        "exclude": exclude,
        "exclusion_reason": reason,
    }


@task
def update_invoices_task(invoice: dict) -> dict:
    """
    Updates Invoice status to paid and adds "success" or "failed" to input dict
    If invoice is missing, dict only has key "missing"
    { "<invoice_uuid>": {ap_payment_report_row},
      "invoice": {folio_invoice_data},
      "missing": {ap_payment_report_row},
      "cancelled": {ap_payment_report_row},
      "paid": {ap_payment_report_row},
      "success": {folio_invoice_data},
      "failed": {ap_payment_report_row}
    }
    """
    invoice_to_update = invoice.get("invoice")
    if invoice_to_update is None:
        logger.info("No invoice to update")
        return invoice
    else:
        folio_client = _folio_client()
        invoice_to_update["status"] = "Paid"
        invoice_id = invoice_to_update["id"]
        logger.info(f"Updating Invoice {invoice_id}")
        try:
            folio_client.folio_put(f"/invoice/invoices/{invoice_id}", invoice_to_update)
            logger.info(f"Updated {invoice_id} to status of Paid")
            invoice["success"] = invoice_to_update
            return invoice
        except httpx.HTTPError as e:
            logger.error(f"Failed to update invoice {invoice_id}: {e}")
            report_row = invoice[invoice_id]
            report_row["invoice_id"] = invoice_id
            invoice["failed"] = report_row

            return invoice


@task
def update_vouchers_task(voucher: dict, invoice: dict) -> Union[dict, None]:
    """
    Gets retrieve_voucher_task dict: {"missing": "invoice_id", "paid": "", "multiple": "invoice_id", "voucher": {<voucher data>}}
    and update_invoice_task dict: if invoice is missing, dict only has key "missing"
    Adds to voucher dictionary "success" or "failed": {"success": {voucher_to_update}}
    """
    if (
        voucher is None
        or voucher.get("voucher") is None
        or invoice.get("invoice") is None
    ):
        # returns None
        raise AirflowSkipException("Cannot update voucher")
    else:
        voucher_to_update = voucher["voucher"]
        folio_client = _folio_client()
        voucher_id = voucher_to_update["id"]
        logger.info(f"Updating voucher {voucher_id}")
        report_row = invoice[(voucher_to_update["invoiceId"])]
        voucher_to_update["status"] = (
            "Paid"  # should already be Paid when update_invoice task ran
        )
        voucher_to_update["disbursementAmount"] = report_row["AmountPaid"]
        voucher_to_update["disbursementNumber"] = report_row["PaymentNumber"]
        disbursement_date = datetime.strptime(report_row["PaymentDate"], "%m/%d/%Y")
        voucher_to_update["disbursementDate"] = disbursement_date.isoformat()
        try:
            folio_client.folio_put(f"/voucher/vouchers/{voucher_id}", voucher_to_update)
            logger.info(f"Updated {voucher_id}")
            voucher["success"] = voucher_to_update
        except httpx.HTTPError:
            logger.warning(f"Failed to update voucher {voucher_id}")
            voucher["failed"] = voucher_to_update

        return voucher
