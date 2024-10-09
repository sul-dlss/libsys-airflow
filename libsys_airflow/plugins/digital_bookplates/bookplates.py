import logging

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import Session
from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate

logger = logging.getLogger(__name__)


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
    Trigger add a tag dag with instance UUIDs and fund 979 data. Returns a dict:
    {
        "242c6000-8485-5fcd-9b5e-adb60788ca59": [
            { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
            { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
        ],
        "bd65eb13-739b-4aa8-baaa-9c1ea48f4c33": [
            { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
            { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
        ]
    }
    """


@task
def construct_979_marc_tags(druid_instances: dict) -> dict:
    """
    get the bookplate data from the bookplates table and contruct a 979 tag with the
    fund name in subfield f, druid in subfield b, image filename in subfield c, and title in subfield d:
    {
        '979': {'ind1': ' ', 'ind2': ' ', 'subfields': [
                {'f': 'ABBOTT'}, {'b': 'druid:ws066yy0421'}, {'c': 'ws066yy0421_00_0001.jp2'}, {'d': 'The The Donald P. Abbott Fund for Marine Invertebrates'}
            ]
        }
    }
    """
    return {}
