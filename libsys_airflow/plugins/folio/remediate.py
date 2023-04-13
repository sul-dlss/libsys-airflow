"""Module for remediation of failed records migration"""
import logging
import pathlib
import sqlite3

import requests

from airflow.operators.python import get_current_context
from folioclient import FolioClient
from libsys_airflow.plugins.folio.audit import AuditStatus
from folio_uuid.folio_uuid import FOLIONamespaces

logger = logging.getLogger(__name__)


def _get_json_payload(con: sqlite3.Connection, record_id: int) -> dict:
    """Retrieves JSON Payload from Audit Database for missing Record"""
    cur = con.cursor()
    cur.execute("SELECT payload FROM JsonPayload WHERE record_id=?;", (record_id,))
    record_json = cur.fetchone()
    if record_json:
        return record_json[0]


def _post_record(**kwargs):
    post_url: str = kwargs["post_url"]
    db_id: int = kwargs["db_id"]
    con: sqlite3.Connection = kwargs["con"]
    okapi_headers: dict = kwargs["okapi_headers"]

    record = _get_json_payload(con, db_id)
    if record is None:
        logger.error("Could not retrieve Payload for Record")
        return
    post_result = requests.post(post_url, data=record, headers=okapi_headers)
    if post_result.status_code != 201:
        _save_error(con, db_id, post_result)


def _retrieve_missing_records(con: sqlite3.Connection, record_type: int) -> list:
    """Retrieves missing record ids"""
    cur = con.cursor()

    cur.execute(
        """SELECT Record.id FROM Record, AuditLog
    WHERE Record.id = AuditLog.record_id
    AND Record.folio_type=?
    AND AuditLog.status=?""",
        (record_type, AuditStatus.MISSING.value),
    )
    missing_records = cur.fetchall()
    cur.close()
    return missing_records


def _save_error(
    con: sqlite3.Connection,
    db_id: int,
    post_response,
):
    """Saves Error to Audit Database"""
    cur = con.cursor()
    cur.execute(
        "INSERT INTO Errors (log_id, message, http_status_code) VALUES (?,?,?);",
        (db_id, post_response.text, post_response.status_code),
    )
    con.commit()
    cur.close()


def _add_missing_holdings(con: sqlite3.Connection, folio_client: FolioClient):
    """Adds Missing Holdings Records"""
    logger.info("Starting POSTs for Missing Holdings to FOLIO")
    holdings_url = f"{folio_client.okapi_url}/holdings-storage/holdings"
    missing_holdings_ids = _retrieve_missing_records(
        con, FOLIONamespaces.holdings.value
    )
    count = 0
    for holdings_row in missing_holdings_ids:
        if not count % 1_000:
            logger.info(f"Added {count:,} Holdings to FOLIO")
        _post_record(
            post_url=holdings_url,
            db_id=holdings_row[0],
            con=con,
            okapi_headers=folio_client.okapi_headers,
        )
        count += 1
    logger.info(f"Finished adding {count:,} Holdings")


def _add_missing_instances(con: sqlite3.Connection, folio_client: FolioClient):
    """Adds Missing Instances"""
    logger.info("Starting POSTs for Missing Instances to FOLIO")
    instance_url = f"{folio_client.okapi_url}/instance-storage/instances"
    missing_instance_ids = _retrieve_missing_records(
        con, FOLIONamespaces.instances.value
    )
    count = 0
    for instance_row in missing_instance_ids:
        if not count % 1_000:
            logger.info(f"Added {count:,} Instances to FOLIO")
        _post_record(
            post_url=instance_url,
            db_id=instance_row[0],
            con=con,
            okapi_headers=folio_client.okapi_headers,
        )
        count += 1
    logger.info(f"Finished adding {count:,} Instances")


def _add_missing_items(con: sqlite3.Connection, folio_client: FolioClient):
    """Add Missing Items"""
    logger.info("Starting POSTs for Missing Items to FOLIO")
    item_url = f"{folio_client.okapi_url}/item-storage/items"
    missing_items_ids = _retrieve_missing_records(con, FOLIONamespaces.items.value)
    count = 0
    for item_row in missing_items_ids:
        if not count % 1_000:
            logger.info(f"Added {count:,} Items to FOLIO")
        _post_record(
            post_url=item_url,
            db_id=item_row[0],
            con=con,
            okapi_headers=folio_client.okapi_headers,
        )
        count += 1
    logger.info(f"Finished adding {count:,} Items")


def add_missing_records(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    folio_client = kwargs["folio_client"]
    iteration_id = kwargs["iteration_id"]

    audit_db = (
        pathlib.Path(airflow)
        / f"migration/iterations/{iteration_id}/results/audit-remediation.db"
    )

    con = sqlite3.connect(str(audit_db))

    # Determine number of missing records in FOLIO
    cur = con.cursor()
    cur.execute(
        "SELECT count(id) FROM AuditLog WHERE status=?;",
        (AuditStatus.MISSING.value,)
    )
    count_result = cur.fetchone()
    cur.close()
    if count_result[0] < 1:
        logger.info("No missing records found!")
        con.close()
        return

    _add_missing_instances(con, folio_client)
    _add_missing_holdings(con, folio_client)
    _add_missing_items(con, folio_client)

    con.close()


def start_record_qa(*args, **kwargs):
    context = get_current_context()
    params = context.get("params")
    iteration_id = params.get("iteration_id")
    if iteration_id is None:
        raise ValueError("Iteration ID cannot be None")
    logger.info(f"Starting Record QA and Remediation for {iteration_id}")
    return iteration_id
