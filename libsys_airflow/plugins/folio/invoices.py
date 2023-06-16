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


def _get_ids_from_vouchers(folio_query: str, folio_client: FolioClient) -> list:
    """
    Returns all invoice Ids from vouchers given query parameter
    """
    vouchers = folio_client.get(
        "/voucher/vouchers", params={"query": folio_query, "limit": 100}
    )
    return [row.get("invoiceId") for row in vouchers["vouchers"]]


@task
def invoices_awaiting_payment_task() -> list:
    folio_client = _folio_client()
    invoice_ids = _get_ids_from_vouchers(
        "exportToAccounting=true and status=\"Awaiting payment\" and cql.allRecords=1 not disbursementNumber=\"\"",
        folio_client,
    )
    return invoice_ids


@task
def invoices_pending_payment_task(invoice_ids: list, upload_status: bool):
    return "foo"
