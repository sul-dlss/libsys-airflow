import json
import logging
import pathlib
import sqlite3

from enum import Enum

from folioclient import FolioClient
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

class AuditStatus(Enum):
    EXISTS = 1
    MISSING = 2
    ERROR = 3

from folio_uuid.folio_uuid import FOLIONamespaces
        
def _audit_record(
    record,
    record_type,
    con,
    pg_cursor,
    select_sql):
    """Audits FOLIO Record by looking up id from the FOLIO Database"""
    record_db_id = _get_add_record(record, con, record_type)
    pg_cursor.execute(select_sql, (record['id'],))
    check_result = pg_cursor.fetchone()
    status = AuditStatus.EXISTS.value
    if check_result is None:
        # Add JSON record to db for later remediation
        _add_json_record(record, con, record_db_id)
        status = AuditStatus.MISSING.value
    _add_audit_log(record_db_id, con, status)


def _audit_instances(
    results_path: pathlib.Path,
    audit_connection: sqlite3.Connection,
    pg_cursor):
    logger.info("Starting Instances Audit")
    select_sql = "SELECT creation_date FROM sul_mod_inventory_storage.instance WHERE id=%s;"
    with (results_path / "folio_instances_bibs-transformer.json").open() as fo:
        for i,line in enumerate(fo.readlines()):
            instance = json.loads(line)
            _audit_record(
                instance,
                FOLIONamespaces.instances.value,
                audit_connection,
                pg_cursor,
                select_sql)
            if not i%1_000:
                logger.info(f"Audited {i:,} instances")
    logger.info("Finished Auditing {i:,} Instances")
    return i


def _audit_holdings_records(
    results_path: pathlib.Path,
    audit_connection: sqlite3.Connection,
    pg_cursor):
    """Audits FOLIO Holdings through retrieval from the FOLIO Database"""
    logger.info("Staring Holdings Audit")
    select_sql = """SELECT creation_date FROM sul_mod_inventory_storage.holdings_record 
    WHERE id=%s"""
    holdings_count = 0
    holdings_files = [
        (results_path / "folio_holdings_tsv-transformer.json"),
        (results_path / "folio_holdings_electronic-transformer.json"),
        (results_path / "folio_holdings_mhld-transformer.json")
    ]
    for holdings_file in holdings_files:
        logger.info(f"Starting auditing of {holdings_file}")
        with holdings_file.open() as fo:
            for line in fo.readlines():
                holdings = json.loads(line)
                _audit_record(
                    holdings,
                    FOLIONamespaces.holdings.value,
                    audit_connection,
                    pg_cursor,
                    select_sql
                )
                if not holdings_count%1_000:
                    logger.info(f"Audited {holdings_count:,} holdings")
                holdings_count += 1
    logger.info("Finished Auditing {holdings_count:,} Holdings")
    return holdings_count


def _audit_items(
    results_path: pathlib.Path,
    audit_connection: sqlite3.Connection,
    pg_cursor):
    """Audits FOLIO Item through retrieval from the FOLIO Database"""
    logger.info("Starting Items Audit")
    select_sql = "SELECT * FROM sul_mod_inventory_storage.item WHERE id=%s"
    with (results_path / "folio_items_transformer.json").open() as fo:
        for i, line in enumerate(fo.readlines()):
            item = json.loads(line)
            _audit_record(
                item,
                FOLIONamespaces.items.value,
                audit_connection,
                pg_cursor,
                select_sql
            )
            if not i%1_000:
                logger.info(f"Audited {i:,} items")
    logger.info(f"Finished Auditing {i:,} Items")
    return i 
    


def _add_record(record, con, record_type):
    cur = con.cursor()
    cur.execute("""INSERT INTO Record (uuid, hrid, folio_type, current_version) 
                   VALUES (?,?,?,?);""",
            (
                record['id'],
                record['hrid'],
                record_type,
                record["_version"]
            )
    )
    record_id = cur.lastrowid
    con.commit()
    cur.close()
    return record_id


def _add_json_record(record, con, record_db_id):
    """Add full record for later remediation"""
    cur = con.cursor()
    cur.execute("""INSERT INTO JsonPayload (record_id, payload)
                    VALUES (?,?);""",
                (
                    record_db_id,
                    json.dumps(record)
                )
    )
    cur.close()


def _add_audit_log(record_id, con, status_id):
    cur = con.cursor()
    cur.execute("""INSERT INTO AuditLog (record_id, status) 
                   VALUES (?,?);""",
                (
                    record_id, 
                    status_id
                ))
    log_id = cur.lastrowid
    cur.close()
    return log_id
                

def _get_add_record(record, con, record_type):
    cur = con.cursor()
    cur.execute("SELECT id FROM Record WHERE uuid=?;",
        (record['id'],)
    )
    record_exists = cur.fetchone()
    if record_exists:
        return record_exists[0]
    cur.close()
    record_id = _add_record(record, con, record_type)
    return record_id


def _check_instance_views(results_dir, pg_hook) -> dict:
    """
    Associates Instances to it's Holdings and Items. When 
    https://s3.amazonaws.com/foliodocs/api/mod-inventory-storage/p/inventory-view.html
    is available, should use this single call to construct.
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
    logger.info(f"Finished auditing {instance_count:,} instances, {holdings_count:,} holdings and {items_count:,} items")


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
    with open(airflow / "migration/qa.sql") as fo:
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
    results_dir = pathlib.Path(airflow) / f"migration/iterations/{iteration_id}/results/"
    logger.info("Constructing instance views")
    _check_instance_views(results_dir, pg_hook)
    logger.info("Finished audit of Instance views")



