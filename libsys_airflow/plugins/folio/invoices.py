import logging

import httpx

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


def _update_vouchers_to_pending(invoice_ids: list, folio_client: FolioClient) -> dict:
    """
    Iterates through list of invoice ids and updates disbursementNumber to Pending
    """
    success, failures = [], []
    for invoice in invoice_ids:
        logger.info(f"Processing Invoice {invoice['id']}")
        voucher_result = folio_client.get(
            f"/voucher-storage/vouchers?query=(invoiceId=={invoice['id']})"
        )
        for voucher in voucher_result.get("vouchers", []):
            voucher_id = voucher["id"]
            voucher["disbursementNumber"] = "Pending"
            put_result = httpx.put(
                f"{folio_client.okapi_url}/voucher-storage/vouchers/{voucher_id}",
                headers=folio_client.okapi_headers,
                json=voucher,
            )
            if put_result.status_code == 204:
                logger.info(
                    f"Successfully set disbursementNumber for voucher {voucher_id}"
                )
                success.append(voucher_id)
            else:
                logger.error(
                    f"Failed to update {voucher_id} HTTP Error {put_result.status_code} {put_result.text}"
                )
                failures.append(voucher_id)
    return {"success": success, "failures": failures}


@task
def invoices_awaiting_payment_task() -> list:
    """
    Retrieves all awaiting payment invoices uuids from vouchers endpoint
    """
    folio_client = _folio_client()
    sul_invoices = "(acqUnitIds==*\"bd6c5f05-9ab3-41f7-8361-1c1e847196d3\"*) and exportToAccounting=true and status=\"Awaiting payment\" and cql.allRecords=1 not disbursementNumber=\"Pending\""
    law_invoices = "(acqUnitIds==*\"556eb26f-dbea-41c1-a1de-9a88ad950d95\"*) and exportToAccounting=true and status=\"Awaiting payment\" and cql.allRecords=1 not disbursementNumber=\"Pending\""
    query = f"""({sul_invoices}) or ({law_invoices})"""
    invoice_ids = _get_ids_from_vouchers(query, folio_client)
    return invoice_ids


@task
def invoices_pending_payment_task(invoice_ids: list):
    folio_client = _folio_client()
    return _update_vouchers_to_pending(invoice_ids, folio_client)
