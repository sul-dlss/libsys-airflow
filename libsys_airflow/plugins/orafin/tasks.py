import logging
import pathlib

from airflow.sdk import get_current_context, task, Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from folioclient import FolioClient

from libsys_airflow.plugins.orafin.emails import (
    generate_ap_error_report_email,
    generate_ap_paid_report_email,
    generate_excluded_email,
    generate_invoice_error_email,
    generate_summary_email,
)

from libsys_airflow.plugins.orafin.reports import (
    extract_rows,
    filter_files,
    retrieve_invoice,
    retrieve_voucher,
    update_invoice,
    update_voucher,
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


@task(trigger_rule='none_failed')
def email_errors_task(ti=None):
    folio_url = Variable.get("FOLIO_URL")
    total_errors = generate_ap_error_report_email(folio_url, ti)
    return f"Email {total_errors:,} error reports"


@task
def email_excluded_task(invoices_exclusion_reasons: list):
    folio_url = Variable.get("FOLIO_URL")
    if len(invoices_exclusion_reasons) > 0:
        generate_excluded_email(invoices_exclusion_reasons, folio_url)
    return f"Emailed report for {len(invoices_exclusion_reasons):,} invoices"


@task(trigger_rule='none_failed')
def email_paid_task(ti=None):
    folio_url = Variable.get("FOLIO_URL")
    total_invoices = generate_ap_paid_report_email(folio_url, ti)
    return f"Emailed all paid invoices for {folio_url} {total_invoices}"


@task
def email_summary_task(invoices: list):
    folio_url = Variable.get("FOLIO_URL")
    generate_summary_email(invoices, folio_url)
    return f"Emailed summary report for {len(invoices):,} invoices"


@task
def email_invoice_errors_task(ti=None):
    folio_url = Variable.get("FOLIO_URL")
    invoice_id = ti.xcom_pull(task_ids="update_invoices_task")
    generate_invoice_error_email(invoice_id, folio_url, ti)
    return f"Emailed Error for Invoice {invoice_id}"


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
def retrieve_invoice_task(row: dict):
    """
    Retrieves invoice from a row dictionary
    """
    folio_client = _folio_client()
    return retrieve_invoice(row, folio_client)


@task
def retrieve_voucher_task(ti=None):
    """
    Retrieves voucher based on invoice id
    """
    invoice_id: str = ti.xcom_pull(
        task_ids="update-folio.update_invoices_task", map_indexes=ti.map_index
    )
    folio_client = _folio_client()
    return retrieve_voucher(invoice_id, folio_client)


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
def update_invoices_task(invoice: dict):
    if invoice:
        folio_client = _folio_client()
        logger.info(f"Updating Invoice {invoice['id']}")
        update_invoice(invoice, folio_client)
        return invoice['id']
    else:
        logger.error("Invoice is None")


@task.branch()
def update_email_branch(update_result):
    if update_result is False:
        return "update-folio.email_invoice_errors"
    return "update-folio.retrieve_voucher_task"


@task
def update_vouchers_task(ti=None):
    voucher = ti.xcom_pull(
        task_ids="update-folio.retrieve_voucher_task", map_indexes=ti.map_index
    )
    if voucher:
        folio_client = _folio_client()
        logger.info(f"Updating voucher {voucher['id']}")
        voucher = update_voucher(voucher, ti, folio_client)
        return voucher['id']
    else:
        logger.error("Voucher is None")
