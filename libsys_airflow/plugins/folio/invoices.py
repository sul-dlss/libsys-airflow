import logging

import httpx

from airflow.decorators import task
from airflow.models import Variable

from folioclient import FolioClient

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
    vouchers = folio_client.folio_get(
        "/voucher/vouchers",
        key="vouchers",
        query_params={"query": folio_query, "limit": 500},
    )
    return [row.get("invoiceId") for row in vouchers]


def _get_all_ids_from_invoices(folio_query: str, folio_client: FolioClient) -> list:
    """
    Returns all invoice Ids from invoices given query parameter in `limit`-size chunks
    """
    invoices = folio_client.folio_get_all(
        "/invoice/invoices", key="invoices", query=folio_query, limit=500
    )
    return [row.get("id") for row in invoices]


def _get_all_invoice_lines(folio_query: str, folio_client: FolioClient) -> list:
    """
    Returns all invoice line given a query parameter in `limit`-size chunks
    """
    invoice_lines = folio_client.folio_get_all(
        "/invoice/invoice-lines", key="invoiceLines", query=folio_query, limit=500
    )
    return [row for row in invoice_lines]


def _update_vouchers_to_pending(invoice_ids: list, folio_client: FolioClient) -> dict:
    """
    Iterates through list of invoice ids and updates disbursementNumber to Pending
    """
    success, failures = [], []
    for invoice in invoice_ids:
        logger.info(f"Processing Invoice {invoice['id']}")
        voucher_result = folio_client.folio_get(
            f"/voucher-storage/vouchers?query=(invoiceId=={invoice['id']})"
        )
        for voucher in voucher_result.get("vouchers", []):
            voucher_id = voucher["id"]
            voucher["disbursementNumber"] = "Pending"
            try:
                folio_client.folio_put(
                    f"/voucher-storage/vouchers/{voucher_id}",
                    payload=voucher,
                )
                logger.info(
                    f"Successfully set disbursementNumber for voucher {voucher_id}"
                )
                success.append(voucher_id)
            except httpx.HTTPError as e:
                logger.error(f"Failed to update {voucher_id} HTTP Error {e}")
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
    business_invoices = "(acqUnitIds==*\"c74ceb20-33fb-4b50-914e-a056db67feea\"*) and exportToAccounting=true and status=\"Awaiting payment\" and cql.allRecords=1 not disbursementNumber=\"Pending\""
    query = f"""({sul_invoices}) or ({law_invoices}) or ({business_invoices})"""
    invoice_ids = _get_ids_from_vouchers(query, folio_client)
    return invoice_ids


@task
def invoices_pending_payment_task(invoice_ids: list):
    folio_client = _folio_client()
    return _update_vouchers_to_pending(invoice_ids, folio_client)


@task
def invoices_paid_within_date_range(**kwargs) -> list:
    """
    Get invoices with status=Paid and paymentDate=<range>, return invoice UUIDs
    paymentDate range based on airflow DAG run data intervals end and start dates
    Query paymentDate greater than logical_date when run_id starts with "manual_"
    """
    folio_client = _folio_client()
    dag_run = kwargs["dag_run"]
    dag_run_id = dag_run.run_id
    from_date = dag_run.data_interval_start
    to_date = dag_run.data_interval_end
    query = f"""?query=((paymentDate>={from_date} and paymentDate<={to_date}) and status=="Paid")"""
    if dag_run_id.startswith("manual_"):
        logger.info(f"Querying paid invoices with paymentDate >= {from_date}")
        from_date = dag_run.logical_date
        query = f"""?query=((paymentDate>={from_date}) and status=="Paid")"""

    else:
        logger.info(
            f"Querying paid invoices with paymentDate range >= {from_date} and <= {to_date}"
        )

    invoice_ids = _get_all_ids_from_invoices(query, folio_client)
    return invoice_ids


@task
def invoice_lines_from_invoices(invoices: list) -> list:
    """
    Given a list of invoice UUIDs, returns a list of invoice lines dictionaries
    """
    folio_client = _folio_client()
    all_invoice_lines = []
    for id in invoices:
        logger.info(f"Getting invoice lines for {id}")
        query = f"""?query=(invoiceId=={id})"""
        invoice_lines = _get_all_invoice_lines(query, folio_client)
        for row in invoice_lines:
            all_invoice_lines.append(row)

    return all_invoice_lines
