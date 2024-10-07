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


def _get_bookplate_metadata_with_fund_uuids() -> dict:
    funds = {}
    pg_hook = PostgresHook("digital_bookplates")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        for f in (
            session.query(DigitalBookplate)
            .where(DigitalBookplate.fund_uuid.is_not(None))
            .all()
        ):
            funds[f.fund_uuid] = {
                "fund_name": f.fund_name,
                "druid": f.druid,
                "image_filename": f.image_filename,
                "title": f.title,
            }

    return funds


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
    This task needs to get digital bookplates data from the table and
    return a list of bookplates metadata and poline ids
    Input data struct:
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
    Returns:
    [
      {
        "bookplate_metadata": { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
        "poline_id": "798596da-12a6-4c6d-8d3a-3bb6c54cb2f1"
      },
      {
        "bookplate_metadata": { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
        "poline_id": "7a5888fe-689b-4cfe-a27d-c2675a235203"
      }
    ]
    """
    bookplates_polines: list = []
    bookplates = _get_bookplate_metadata_with_fund_uuids()
    # create new list of funds and poline objects (removes invoice lines and empty values)
    funds_polines = []
    for row in invoice_lines:
        for k, v in row.items():
            if row[k]:
                funds_polines.append(v)

    # check for fund id in bookplates dict and add poline id to new dict
    for row in funds_polines:
        funds = row.get("fund_ids")
        poline_id = row.get("poline_id")
        bp_poline_dict = {}
        for f in funds:
            md = bookplates.get(f)
            if f and md is not None:
                bp_poline_dict.update({"bookplate_metadata": md})
                bp_poline_dict.update({"poline_id": poline_id})

        bookplates_polines.append(bp_poline_dict)

    return bookplates_polines


@task
def launch_add_979_fields_task(**kwargs):
    """
    Trigger add a tag dag with instance UUIDs and fund 979 data
    """
