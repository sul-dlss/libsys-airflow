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


def _new_bookplates(funds: list) -> dict:
    """
    Transforms new funds list into dictionary with fund_uuid as key
    """
    bookplates = {}
    for row in funds:
        bookplates[row["fund_uuid"]] = {
            "fund_name": row["fund_name"],
            "druid": row["druid"],
            "image_filename": row["image_filename"],
            "title": row["title"],
        }

    return bookplates


@task
def bookplate_funds_polines(invoice_lines: list, funds: list) -> list:
    """
    Checks if fund Id from invoice lines contains bookplate fund
    This task gets digital bookplates data from the table or uses
    a list of new funds and returns a list of bookplates metadata and poline ids
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
    if len(funds) > 0:
        bookplates = _new_bookplates(funds)
    else:
        bookplates = _get_bookplate_metadata_with_fund_uuids()

    for row in invoice_lines:
        fund_distribution = row.get("fundDistributions")
        poline_id = row.get("poLineId")
        if fund_distribution and poline_id:
            for fund in fund_distribution:
                bookplate = bookplates.get(fund["fundId"])
                if bookplate:
                    bookplates_polines.append(
                        {"bookplate_metadata": bookplate, "poline_id": poline_id}
                    )

    return bookplates_polines


@task
def launch_add_979_fields_task(**kwargs):
    """
    Trigger add a tag dag with instance UUIDs and fund 979 data
    """
