import logging

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
def bookplate_fund_ids(**kwargs) -> dict:
    """
    Looks up in bookplates table for fund_name
    Queries folio for fund_name
    Returns list of fund UUIDs
    """
    # {"druid": "fund_uuid"}
    funds: dict = {}
    return funds


@task
def launch_add_979_fields_task(**kwargs):
    """
    Trigger add a tag dag with instance UUIDs and fund 979 data
    """
