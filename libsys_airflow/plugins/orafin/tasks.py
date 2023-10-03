import logging
import pathlib

from airflow.decorators import task
from airflow.models import Variable

from libsys_airflow.plugins.folio.folio_client import FolioClient

from libsys_airflow.plugins.orafin.emails import (
    generate_excluded_email,
    generate_summary_email,
)

from libsys_airflow.plugins.orafin.ap_reports import (
    email_reporting_errors,
    retrieve_invoice,
    retrieve_rows,
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
def email_excluded_task(invoices: list):
    folio_url = Variable.get("FOLIO_URL")
    if len(invoices) > 0:
        generate_excluded_email(invoices, folio_url)
    return f"Emailed report for {len(invoices):,} invoices"

@task
def email_errors_task():
    folio_url = Variable.get("FOLIO_URL")
    total_errors = email_reporting_errors(folio_url)
    return f"Email {total_errors:,} error reports"

@task
def email_summary_task(invoices: list):
    folio_url = Variable.get("FOLIO_URL")
    generate_summary_email(invoices, folio_url)
    return f"Emailed summary report for {len(invoices):,} invoices"


def extract_rows_task(retrieved_csv: str):
    return retrieve_rows(retrieved_csv)


@task(multiple_outputs=True)
def filter_invoices_task(invoices: list):
    feeder_file, excluded = [], []
    for row in invoices:
        if row['exclude'] is True:
            excluded.append(row['invoice'])
        else:
            feeder_file.append(row['invoice'])
    return {"feed": feeder_file, "excluded": excluded}


@task(max_active_tis_per_dag=5)
def transform_folio_data_task(invoice_id: str):
    """
    Takes Invoice ID and retrieves invoice information and tax status
    from the invoice's organization
    """
    folio_client = _folio_client()
    converter = models_converter()
    # Call to Okapi invoice endpoint
    invoice, exclude = get_invoice(invoice_id, folio_client, converter)
    return {"invoice": converter.unstructure(invoice), "exclude": exclude}


@task
def feeder_file_task(invoices: list):
    converter = models_converter()
    folio_client = _folio_client()
    feeder_file = init_feeder_file(invoices, folio_client, converter)

    return converter.unstructure(feeder_file)






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


# @task -- When SFTP is available on AP server, uncomment this line to make a taskflow task
def sftp_file_task(feeder_file_path: str, sftp_connection: str = None):  # type: ignore
    bash_operator = transfer_to_orafin(feeder_file_path)
    return bash_operator

@task
def retrieve_report_task(sftp_connection: str):
    return "xxdl_ap_payment_09282023161640.csv"


@task(max_active_tis_per_dagrun=5)
def retrieve_invoice_task(row: dict):
    """
    Retrieves invoice from a row dictionary
    """
    folio_client = _folio_client()
    return retrieve_invoice(row, folio_client)


@task
def retrieve_voucher_task(invoice_id: str):
    """
    Retrieves voucher based on invoice id
    """
    folio_client = _folio_client()
    return retrieve_voucher(invoice_id, folio_client)


@task
def update_invoices_task(invoice: dict):
    if invoice:
        logger.info(f"Updating Invoice {invoice['id']}")
        update_invoice(invoice)
        return invoice['id']
    else:
        logger.error("Invoice is None")


@task
def update_vouchers_task(voucher: dict):
    if voucher:
        logger.info(f"Updating voucher {voucher['id']}")
        update_voucher(voucher)
    else:
        logger.error("Voucher is None")
