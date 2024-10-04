import logging

from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import Session
from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate
from folioclient import FolioClient

logger = logging.getLogger(__name__)


def _folio_client():
    return FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )


@task
def bookplate_fund_ids(**kwargs) -> dict:
    """
    Looks up in bookplates table for fund_name
    Queries folio for fund_name
    Returns dict of fund UUIDs
    """
    folio_client = _folio_client()
    folio_funds = folio_client.folio_get(
        "/finance-storage/funds", query_params={"limit": 2999}
    )

    pg_hook = PostgresHook("digital_bookplates")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        fund_tuples = (
            session.query(DigitalBookplate.fund_name, DigitalBookplate.druid).where(
                DigitalBookplate.fund_name.is_not(None)
            )
        ).all()

    fund_names = [n[0] for n in fund_tuples]
    fund_druids = [n[1] for n in fund_tuples]

    funds: dict = {}
    for fund in folio_funds['funds']:
        if fund['name'] in fund_names:
            idx = fund_names.index(fund['name'])
            funds[fund_druids[idx]] = fund['id']

    return funds


@task
def bookplate_fund_po_lines(invoice_lines: list) -> list:
    """
    Checks if fund Id from invoice lines data struct contains bookplate fund
    This task needs to lookup in the digital bookplates table the fund_id
    and remove from the list objects that are not in the bookplates table
    [
      {
        "fadacf66-8813-4759-b4d3-7d506db38f48": {
          "fund_ids": [
            "0e8804ca-0190-4a98-a88d-83ae77a0f8e3"
          ],
          "poline_id": "b5ba6538-7e04-4be3-8a0e-c68306c355a2"
        }
      }
    ]
    Returns po lines and fund ids that are bookplate funds
    """
    funds_invoice_lines: list = []

    return funds_invoice_lines


@task
def launch_add_979_fields_task(**kwargs):
    """
    Trigger add a tag dag with instance UUIDs and fund 979 data
    """
