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

def add_missing_records(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    task_instance = kwargs["task_instance"]

    audit_db = pathlib.Path(airflow) / f"migration/iterations/{task_instance}/results/"


def start_record_qa(*args, **kwargs):
    context = get_current_context()
    params = context.get("params")
    iteration_id = params.get("iteration_id")
    if iteration_id is None:
        raise ValueError(f"Iteration ID cannot be None")
    logger.info(f"Starting Record QA and Remediation for {iteration_id}")
    return iteration_id


