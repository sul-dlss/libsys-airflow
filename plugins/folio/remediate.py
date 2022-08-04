"""Module for remediation of failed records migration"""
import datetime
import json
import logging
import pathlib

import requests

from folioclient import FolioClient


def _error_file(error_file: pathlib.Path):
    with open(error_file) as fo:
        for line in fo.readlines():
            yield json.loads(line)


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
        post_result = requests.post(post_url, headers=folio_client.okapi_headers, json=record)
        if post_result.status_code < 300:
            logging.info(f"Added {record['id']} to {folio_client.okapi_url}")
        else:
            logging.error(f"Failed to POST {record['id']} to {folio_client.okapi_url} - {post_result.status_code}\n{post_result.text}")
    else:
        logging.error(
            f"Failed to PUT {record['id']} - {put_result.status_code}\n{put_result.text}"
        )


def handle_record_errors(*args, **kwargs):
    dag = kwargs["dag_run"]
    base = kwargs["base"]
    endpoint = kwargs["endpoint"]
    folio_client = kwargs['folio_client']

    airflow = kwargs.get("airflow", "/opt/airflow")
    airflow_path = pathlib.Path(airflow)

    results_dir = airflow_path / "migration/results/"

    pattern = f"errors-{base}-*-{dag.run_id}.json"

    for error_file in results_dir.glob(pattern):
        logging.info(f"Processing error file {error_file} ")
        for record in _error_file(error_file):
            if _is_missing_or_outdated(record, endpoint, folio_client):
                _post_or_put_record(record, endpoint, folio_client)

    logging.info(f"Finished error handling for {base} errors")
