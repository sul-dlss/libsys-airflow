
import logging
import requests
import pathlib
from airflow.models import Variable

logger = logging.getLogger(__name__)


def FolioLogin(**kwargs):
    """Logs into FOLIO and returns Okapi token."""
    okapi_url = Variable.get("OKAPI_URL")
    username = Variable.get("FOLIO_PASSWORD")
    password = Variable.get("FOLIO_USER")
    tenant = "sul"

    data = {"username": username, "password": password}
    headers = {"Content-type": "application/json", "x-okapi-tenant": tenant}

    url = f"{okapi_url}/authn/login"
    result = requests.post(url, json=data, headers=headers)

    if result.status_code == 201:  # Valid token created and returned
        return result.headers.get("x-okapi-token")

    result.raise_for_status()

def _post_to_okapi(**kwargs):
    endpoint = kwargs.get('endpoint')
    jwt = FolioLogin(**kwargs)

    records = kwargs["records"]
    tenant = "sul"
    okapi_url = Variable.get("OKAPI_URL")

    okapi_instance_url = f"{okapi_url}{endpoint}"

    headers = {
        "Content-type": "application/json",
        "user-agent": "FolioAirflow",
        "x-okapi-token": jwt,
        "x-okapi-tenant": tenant,
    }

    payload = {"instances": records}

    new_record_result = requests.post(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    logger.info(new_record_result.status_code)

# Maybe check for empty files here...
# Also each json file is just an object on each line, not an array
# Parse each json file and add to (comma separated) array that gets posted as the payload on line 45.
def post_folio_instance_records(**kwargs):
    """Creates new records in FOLIO"""
    inventory_records = [fo.read_text() for fo in pathlib.Path(
        "/opt/airflow/migration/results").glob("folio_instance_*.json"
        )]

    _post_to_okapi(records=inventory_records,
                   endpoint="/instance-storage/batch/synchronous?upsert=true", **kwargs)
