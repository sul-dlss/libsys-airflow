import logging

from folioclient import FolioClient

logger = logging.getLogger(__name__)


def current_fiscal_years(ledgers: list, folio_client: FolioClient) -> list:
    """
    Returns a list of current fiscal year UUIDs given a list ledger UUIDs
    """
    current_fy_ids = []
    for id in ledgers:
        fy_code = folio_client.folio_get(
            f"/finance/ledgers/{id}/current-fiscal-year"
        ).get("code")
        if fy_code is not None:
            fiscal_years = folio_client.folio_get(
                "/finance/fiscal-years", query_params={"query": f"code=={fy_code}"}
            )["fiscalYears"]
            fy_id = fiscal_years[0].get("id")
            if fy_id is not None:
                current_fy_ids.append(fy_id)

    return current_fy_ids


def active_ledgers(folio_client: FolioClient) -> list:
    """
    Returns a list of ledger UUIDs that are status Active
    """
    ledgers = folio_client.folio_get(
        "/finance/ledgers", query_params={"query": "ledgerStatus==Active", "limit": 500}
    )
    return [row.get("id") for row in ledgers["ledgers"]]
