import logging

from airflow.decorators import task
from airflow.models import Variable

from libsys_airflow.plugins.folio.folio_client import FolioClient

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


@task(max_active_tis_per_dag=5)
def transform_folio_data_task(invoice_id: str):
    """
    Takes Invoice ID and retrieves invoice information and tax status
    from the invoice's organization
    """
    folio_client = _folio_client()
    # Call to Okapi invoice endpoint
    invoice = _get_invoice(invoice_id, folio_client)
    return invoice


def _get_invoice(invoice_id: str, folio_client: FolioClient) -> dict:
    """
    Retrieves Invoice, Invoice Lines, and checks for vendor's VAT status
    """
    invoice = {"vendorId": None}
    # Retrieves Invoice Details
    # Retrieves Invoices Lines
    # Call to Okapi organization endpoint to see VAT is applicable
    invoice["vat"] = _get_vat(invoice["vendorId"], folio_client)
    return invoice


def _get_vat(organization_id: str, folio_client: FolioClient) -> bool:
    """
    Retrieves an organization and checks if VAT applies
    """
    return False


@task
def feeder_file_task(invoices: list):
    return "foo"


@task
def sftp_file_task(feeder_file: task):
    return "foo"
