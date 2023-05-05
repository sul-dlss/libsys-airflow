import json
import logging
import pathlib
import sqlite3

from enum import Enum

from airflow.providers.postgres.hooks.postgres import PostgresHook
from folio_uuid.folio_uuid import FOLIONamespaces

logger = logging.getLogger(__name__)


class AuditStatus(Enum):
    EXISTS = 1
    MISSING = 2
    ERROR = 3


def _audit_record(**kwargs):
    """Audits FOLIO Record by looking up id from the FOLIO Database"""
    record = kwargs["record"]
    record_type = int(kwargs["record_type"])
    audit_con= kwargs["audit_con"]
    pg_cursor = kwargs["pg_cursor"]
    sql = kwargs["sql"]

    record_db_id = _get_add_record(record, audit_con, record_type)
    pg_cursor.execute(sql, (record["id"],))

    status = AuditStatus.EXISTS.value
    check_result = pg_cursor.fetchone()
    if check_result is None:
        # Add JSON record to db for later remediation
        _add_json_record(record, audit_con, record_db_id)
        status = AuditStatus.MISSING.value

    _add_audit_log(record_db_id, audit_con, status)


def _audit_instances(
    results_path: pathlib.Path, audit_connection: sqlite3.Connection, pg_cursor
):
    logger.info("Starting Instances Audit")
    select_sql = (
        "SELECT creation_date FROM sul_mod_inventory_storage.instance WHERE id=%s;"
    )
    instance_count = 0
    with (results_path / "folio_instances_bibs-transformer.json").open() as fo:
        for line in fo.readlines():
            instance = json.loads(line)
            _audit_record(
                record=instance,
                record_type=FOLIONamespaces.instances.value,
                audit_con=audit_connection,
                pg_cursor=pg_cursor,
                sql=select_sql,
            )
            if not instance_count % 1_000:
                logger.info(f"Audited {instance_count:,} instances")
            instance_count += 1
    logger.info(f"Finished Auditing {instance_count:,} Instances")
    return instance_count


def _audit_holdings_records(
    results_path: pathlib.Path, audit_connection: sqlite3.Connection, pg_cursor
):
    """Audits FOLIO Holdings through retrieval from the FOLIO Database"""
    logger.info("Staring Holdings Audit")
    select_sql = """SELECT creation_date FROM sul_mod_inventory_storage.holdings_record
     WHERE id=%s"""
    holdings_count = 0

    with (results_path / "folio_holdings.json").open() as fo:
        for line in fo.readlines():
            holdings = json.loads(line)
            _audit_record(
                record=holdings,
                record_type=FOLIONamespaces.holdings.value,
                audit_con=audit_connection,
                pg_cursor=pg_cursor,
                sql=select_sql,
            )
            if not holdings_count % 1_000:
                logger.info(f"Audited {holdings_count:,} holdings")
            holdings_count += 1
    logger.info(f"Finished Auditing {holdings_count:,} Holdings")
    return holdings_count


def _audit_items(
    results_path: pathlib.Path, audit_connection: sqlite3.Connection, pg_cursor
):
    """Audits FOLIO Item through retrieval from the FOLIO Database"""
    logger.info("Starting Items Audit")
    select_sql = "SELECT * FROM sul_mod_inventory_storage.item WHERE id=%s"
    item_count = 0
    with (results_path / "folio_items_transformer.json").open() as fo:
        for line in fo.readlines():
            item = json.loads(line)
            _audit_record(
                record=item,
                record_type=FOLIONamespaces.items.value,
                audit_con=audit_connection,
                pg_cursor=pg_cursor,
                sql=select_sql,
            )
            if not item_count % 1_000:
                logger.info(f"Audited {item_count:,} items")
            item_count += 1
    logger.info(f"Finished Auditing {item_count:,} Items")
    return item_count


def _add_record(record, con, record_type):
    cur = con.cursor()
    cur.execute(
        """INSERT INTO Record (uuid, hrid, folio_type, current_version)
           VALUES (?,?,?,?);""",
        (record["id"], record["hrid"], record_type, record["_version"]),
    )
    record_id = cur.lastrowid
    con.commit()
    cur.close()
    return record_id


def _add_json_record(record, con, record_db_id):
    """Add full record for later remediation"""
    if isinstance(record, dict):
        record_str = json.dumps(record)
    elif isinstance(record, str):
        record_str = record
    else:
        logger.error(f"Unknown record format {type(record)}")
        return
    cur = con.cursor()
    try:
        cur.execute(
            """INSERT INTO JsonPayload (record_id, payload)
                        VALUES (?,?);""",
            (record_db_id, record_str),
        )
    except sqlite3.IntegrityError as err:
        logger.error(f"{err} = {record_db_id}")
    cur.close()


def _add_audit_log(record_id, con, status_id):
    cur = con.cursor()
    cur.execute(
        """INSERT INTO AuditLog (record_id, status)
           VALUES (?,?);""",
        (record_id, status_id),
    )
    log_id = cur.lastrowid
    cur.close()
    return log_id


def _get_add_record(record, con, record_type):
    cur = con.cursor()
    cur.execute("SELECT id FROM Record WHERE uuid=?;", (record["id"],))
    record_exists = cur.fetchone()
    if record_exists:
        return record_exists[0]
    cur.close()
    record_id = _add_record(record, con, record_type)
    return record_id


def _check_instance_views(results_dir, pg_hook) -> dict:
    """
    Associates Instances to it's Holdings and Items.
    """
    pg_connection = pg_hook.get_conn()
    pg_cursor = pg_connection.cursor()
    con = sqlite3.connect(results_dir / "audit-remediation.db")
    instance_count = _audit_instances(results_dir, con, pg_cursor)
    holdings_count = _audit_holdings_records(results_dir, con, pg_cursor)
    items_count = _audit_items(results_dir, con, pg_cursor)
    pg_cursor.close()
    pg_connection.close()
    con.close()
    logger.info(
        f"Finished auditing {instance_count:,} instances, {holdings_count:,} holdings and {items_count:,} items"
    )


def setup_audit_db(*args, **kwargs):
    """
    Initializes the DAG Run Audit SQLITE Database
    """
    airflow: pathlib.Path = pathlib.Path(kwargs.get("airflow", "/opt/airflow"))
    iteration_id = kwargs["iteration_id"]
    results_dir = airflow / f"migration/iterations/{iteration_id}/results"
    db_file = results_dir / "audit-remediation.db"

    if db_file.exists():
        logger.info(f"SQLite Database {db_file} already exists")
        return

    con = sqlite3.connect(str(db_file.absolute()))
    cur = con.cursor()
    # Create Audit Run Database
    with open(airflow / "qa.sql") as fo:
        sql_init = fo.read()
    cur.executescript(sql_init)
    con.commit()
    cur.close()
    con.close()


def audit_instance_views(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow/")
    task_instance = kwargs["task_instance"]
    postgres_connect = kwargs.get("connection", "postgres_folio")
    database = kwargs.get("database", "okapi")
    pg_hook = PostgresHook(postgres_conn_id=postgres_connect, database=database)

    iteration_id = task_instance.xcom_pull(task_ids="start-check-add")
    results_dir = (
        pathlib.Path(airflow) / f"migration/iterations/{iteration_id}/results/"
    )
    logger.info("Constructing instance views")
    _check_instance_views(results_dir, pg_hook)
    logger.info("Finished audit of Instance views")
