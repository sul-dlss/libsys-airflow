from airflow.models import Variable
from folioclient import FolioClient

def folio_client(**kwargs):
    client = kwargs.get("client")

    if client is None:
        client = FolioClient(
            Variable.get("OKAPI_URL", "http://example:9130"),
            "sul",
            Variable.get("FOLIO_USER", "nausername"),
            Variable.get("FOLIO_PASSWORD", "napassword"),
        )

    return client
