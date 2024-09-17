import logging

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
def instances_from_po_lines(po_lines: list, funds: dict) -> dict:
    """
    Given a list of po lines, retrieves the instanceId and fundId from fundDistribution
    Using the fund dict, we lookup the druid using the fundId from the po lines.
    It is possible for and instance to multiple 979's
    """
    # {"instanceUUID": ["druid", "druid"]}
    return {}
