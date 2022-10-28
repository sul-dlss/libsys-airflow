"""Module for remediation of failed records migration"""
import datetime
import json
import logging
import pathlib

import requests

from airflow.models import Variable
from airflow.operators.python import get_current_context

from folioclient import FolioClient

logger = logging.getLogger(__name__)

folio_client = FolioClient(
    Variable.get("OKAPI_URL"),
    "sul",
    Variable.get("FOLIO_USER"),
    Variable.get("FOLIO_PASSWORD"),
)

def _is_missing_or_outdated(
    record: dict, endpoint: str, folio_client: FolioClient
) -> bool:
    """Checks if record is present and updatedDate if present"""
    record_uuid = record.get("id")
    latest_date = datetime.datetime.fromisoformat(record["metadata"]["updatedDate"])
    path = f"{endpoint}/{record_uuid}"

    try:
        folio_record = folio_client.folio_get(path)
        update_date = datetime.datetime.fromisoformat(
            folio_record["metadata"]["updatedDate"]
        )
        return latest_date > update_date
    except Exception as e:
        logging.info(f"{record_uuid} not present in {folio_client.okapi_url}\n{e}")
        return True


def _post_or_put_record(record: dict, endpoint: str, folio_client: FolioClient):
    """Posts record to FOLIO"""
    # Use folio client to extract needed headers and urls
    put_url = f"{folio_client.okapi_url}{endpoint}/{record['id']}"
    post_url = f"{folio_client.okapi_url}{endpoint}"

    # Tries a PUT request for existing record
    put_result = requests.put(put_url, headers=folio_client.okapi_headers, json=record)
    if put_result.status_code < 300:
        logging.info(f"Updated {record['id']} to {folio_client.okapi_url}")
    elif put_result.status_code == 404:
        # Record not found in FOLIO, try creating with POST
        post_result = requests.post(
            post_url, headers=folio_client.okapi_headers, json=record
        )
        if post_result.status_code < 300:
            logging.info(f"Added {record['id']} to {folio_client.okapi_url}")
        else:
            logging.error(
                f"Failed to POST {record['id']} to {folio_client.okapi_url} - {post_result.status_code}\n{post_result.text}"
            )
    else:
        logging.error(
            f"Failed to PUT {record['id']} - {put_result.status_code}\n{put_result.text}"
        )

def _record_exists(record, endoint, ):
    """Checks to see if Record Exists in FOLIO"""
    record_result = requests

def check_add_records(*args, **kwargs):
    task_instance = kwargs["task_instance"]
    records = kwargs["records"]
    endpoint = kwargs["endpoint"]
    folio_client = kwargs["folio_client"]

    airflow = kwargs.get("airflow", "/opt/airflow")
    airflow_path = pathlib.Path(airflow)

    results_dir = airflow_path / f"migration/iterations/{dag.run_id}/results"


    for file_name in records:
        logging.info(f"Processing file {file_name}")
        file_path = results_dir / file_name
        record_count = 0
        for line in file_path.readlines():
            record = json.loads(line)
            if _record_exists(record) is False:
                post_result = _post_record(record, endpoint, folio_client)
                match post_result.status_code:

                    case 400:
                        pass
                    
                    case 422:
                        _handle_422_error(record, endpoint, folio_client)


    logging.info(f"Finished error handling for {base} errors")


def start_record_qa(*args, **kwargs):
    iteration_id = params.get("iteration_id")
    context = get_current_context()
    params = context.get("params")

    logger.info(f"Starting Record QA and Remediation for {iteration_id}")
    return iteration_id


