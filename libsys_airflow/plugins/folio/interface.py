import logging

from airflow.models import Variable

from folioclient import FolioClient


logger = logging.getLogger(__name__)


def interface_info(**kwargs):
    """
    Retrieves the uri and credentials for an interface from the FOLIO API.
    """
    folio_client = kwargs.get("folio_client")

    if folio_client is None:
        folio_client = FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )

    interface_id = kwargs.get("interface_id")

    interface_info = {}
    interface_info['uri'] = folio_client.folio_get(f"/organizations-storage/interfaces/{interface_id}", key='uri')
    credential_response = folio_client.folio_get(f"/organizations-storage/interfaces/{interface_id}/credentials")
    interface_info['username'] = credential_response['username']
    interface_info['password'] = credential_response['password']

    return interface_info
