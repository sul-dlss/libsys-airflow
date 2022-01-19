import logging
import requests
import json
from airflow.models import Variable

logger = logging.getLogger(__name__)


def FolioLogin(**kwargs):
    """Logs into FOLIO and returns Okapi token."""
    okapi_url = Variable.get("OKAPI_URL")
    username = Variable.get("FOLIO_USER")
    password = Variable.get("FOLIO_PASSWORD")
    tenant = "sul"

    data = {"username": username, "password": password}
    headers = {"Content-type": "application/json", "x-okapi-tenant": tenant}

    url = f"{okapi_url}/authn/login"
    result = requests.post(url, json=data, headers=headers)

    if result.status_code == 201:  # Valid token created and returned
        return result.headers.get("x-okapi-token")

    result.raise_for_status()


def _post_to_okapi(**kwargs):
    endpoint = kwargs.get("endpoint")
    jwt = FolioLogin(**kwargs)

    records = kwargs["records"]
    payload_key = kwargs["payload_key"]

    tenant = "sul"
    okapi_url = Variable.get("OKAPI_URL")

    okapi_instance_url = f"{okapi_url}{endpoint}"

    headers = {
        "Content-type": "application/json",
        "user-agent": "FolioAirflow",
        "x-okapi-token": jwt,
        "x-okapi-tenant": tenant,
    }

    payload = {payload_key: records}

    new_record_result = requests.post(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    logger.info(new_record_result.status_code)

    if new_record_result.status_code > 399:
        logger.error(new_record_result.text)
        raise ValueError(
            f"FOLIO POST Failed with error code:{new_record_result.status_code}"
        )


def post_folio_instance_records(**kwargs):
    """Creates new records in FOLIO"""
    with open("/tmp/instances.json") as fo:
        instance_records = json.load(fo)

    _post_to_okapi(
        records=instance_records,
        endpoint="/instance-storage/batch/synchronous?upsert=true",
        payload_key="instances",
        **kwargs,
    )


def post_folio_holding_records(**kwargs):
    """Creates/overlays Holdings records in FOLIO"""
    with open("/tmp/holdings.json") as fo:
        holding_records = json.load(fo)

    _post_to_okapi(
        records=holding_records,
        endpoint="/holdings-storage/batch/synchronous?upsert=true",
        payload_key="holdingsRecords",
        **kwargs,
    )
