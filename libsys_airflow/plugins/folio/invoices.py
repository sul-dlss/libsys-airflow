def _get_vouchers(folio_query, folio_client) -> list:
    """
    Returns all voucher data given query parameter
    """
    return folio_client.folio_get(
        "/voucher-storage/vouchers", params={"query": folio_query}
    )


def invoices_awaiting_payment(folio_client):
    _get_vouchers(
        # doesn't work; should be `(cql.allRecords=1 not disbursementNumber=="")` but that doesn't work either
        "exportToAccounting==true and status==\"Awaiting payment\" and (cql.allRecords=1 and disbursementNumber = \"\")",
        folio_client,
    )


def invoices_pending_payment():
    return "foo"
