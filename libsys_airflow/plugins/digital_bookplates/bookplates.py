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
def bookplate_fund_ids(**kwargs) -> list:
    """
    Looks up in bookplates table for fund_name
    Queries folio for fund_name
    Returns list of fund UUIDs
    """
    folio_client = _folio_client()
    folio_funds = folio_client.folio_get(
        "/finance-storage/funds", query_params={"limit": 2999}
    )

    pg_hook = PostgresHook("digital_bookplates")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        fund_tuples = (
            session.query(DigitalBookplate.fund_name).where(
                DigitalBookplate.fund_name.is_not(None)
            )
        ).all()

    funds: list = []
    fund_names = [n[0] for n in fund_tuples]

    for fund in folio_funds['funds']:
        if fund['name'] in fund_names:
            funds.append(fund['id'])

    return funds


@task
def launch_add_979_fields_task(**kwargs):
    """
    Trigger add a tag dag with instance UUIDs and fund 979 data
    """
