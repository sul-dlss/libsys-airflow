def _get_vouchers(folio_query, folio_client) -> list:
    """
    Returns all voucher data given query parameter
    """
    return folio_client.folio_get(
        "/voucher/vouchers", params={"query": folio_query, "limit": 100}
    )


def invoices_awaiting_payment(folio_client):
    _get_vouchers(
        "exportToAccounting=true and status=\"Awaiting payment\" and cql.allRecords=1 not disbursementNumber=\"\"",
        folio_client,
    )


def invoices_pending_payment():
    return "foo"
