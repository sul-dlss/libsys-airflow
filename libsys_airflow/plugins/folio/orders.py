import logging

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
def instances_from_po_lines(po_lines_funds: list) -> list:
    """
    Given a list of po lines with fund IDs, retrieves the instanceId
    It is possible for and instance to multiple 979's
    [
      {
        "bookplate_metadata": { "druid": "", "fund_name": "", "image_filename": "", "title": "" },
        "instance_id": [
          "242c6000-8485-5fcd-9b5e-adb60788ca59",
          "0a97a746-87ce-58f1-bb61-ffe47b39aff8"
        ]
      }
    ]
    """
    return []
