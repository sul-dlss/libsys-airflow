import logging

from airflow.decorators import task
from airflow.models import Variable

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
def instances_from_po_lines(**kwargs) -> dict:
    """
    Given a list of po lines with fund IDs, retrieves the instanceId
    It is possible for an instance to have multiple 979's
    """
    folio_client = _folio_client()
    instances: dict = {}
    po_lines_funds = kwargs["po_lines_funds"]
    for row in po_lines_funds:
        poline_id = row['poline_id']
        order_line = folio_client.folio_get(f"/orders/order-lines/{poline_id}")
        instance_id = order_line.get("instanceId")
        if instance_id is None:
            logger.info(f"PO Line {poline_id} not linked to a FOLIO Instance record")
            continue
        bookplate_metadata = row["bookplate_metadata"]
        if instance_id in instances:
            instances[instance_id].append(bookplate_metadata)
        else:
            instances[instance_id] = [
                bookplate_metadata,
            ]
    return instances
