import logging

from airflow.models import Variable

from libsys_airflow.plugins.folio.folio_client import FolioClient

logger = logging.getLogger(__name__)


def interface_info(interface_id: str, folio_client=None) -> dict:
    """
    Retrieves the uri and credentials for an interface from the FOLIO API.
    """
    if folio_client is None:
        folio_client = FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )

    interface_resp = folio_client.get(
        f"/organizations-storage/interfaces/{interface_id}"
    )
    credential_resp = folio_client.get(
        f"/organizations-storage/interfaces/{interface_id}/credentials"
    )
    return {
        "uri": interface_resp["uri"],
        "username": credential_resp["username"],
        "password": credential_resp["password"],
    }
