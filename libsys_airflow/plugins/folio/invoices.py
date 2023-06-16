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
        "/voucher/vouchers", params={"query": folio_query, "limit": 500}
    )
    return [row.get("invoiceId") for row in vouchers["vouchers"]]


@task
def invoices_awaiting_payment_task() -> list:
    """
    Retrieves all awaiting payment invoices uuids from vouchers endpoint
    """
    folio_client = _folio_client()
    sul_invoices = "(acqUnitIds==*\"bd6c5f05-9ab3-41f7-8361-1c1e847196d3\"*) and exportToAccounting=true and status=\"Awaiting payment\" and cql.allRecords=1 not disbursementNumber=\"\""
    law_invoices = "(acqUnitIds==*\"556eb26f-dbea-41c1-a1de-9a88ad950d95\"*) and exportToAccounting=true and status=\"Awaiting payment\" and cql.allRecords=1 not disbursementNumber=\"\""
    query = f"""({sul_invoices}) or ({law_invoices})"""
    invoice_ids = _get_ids_from_vouchers(query, folio_client)
    return invoice_ids


@task
def invoices_pending_payment_task(invoice_ids: list, upload_status: bool):
    return "foo"
