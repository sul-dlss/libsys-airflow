import logging
import sqlite3

from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def _db_connection(**kwargs) -> PostgresHook:
    """
    Opens and returns a PostgresHook
    """
    postgres_connect = kwargs.get("connection", "postgres_folio")
    database = kwargs.get("database", "okapi")
    pg_hook = PostgresHook(postgres_conn_id=postgres_connect, database=database)
    return pg_hook


def add_inventory_triggers(**kwargs):
    pg_hook = _db_connection(**kwargs)
    sql = """
          CREATE TRIGGER set_instance_ol_version_trigger
          AFTER INSERT OR UPDATE ON sul_mod_inventory_storage.instance
          FOR EACH ROW EXECUTE FUNCTION sul_mod_inventory_storage.instance_set_ol_version();
          CREATE TRIGGER set_holdings_record_ol_version_trigger
          AFTER INSERT OR UPDATE ON sul_mod_inventory_storage.holdings_record
          FOR EACH ROW EXECUTE FUNCTION sul_mod_inventory_storage.holdings_record_set_ol_version();
          CREATE TRIGGER set_item_ol_version_trigger
          AFTER INSERT OR UPDATE ON sul_mod_inventory_storage.item
          FOR EACH ROW EXECUTE FUNCTION sul_mod_inventory_storage.item_set_ol_version();
    """
    logger.info("Creating mod_inventory_storage triggers")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    logger.info("Finished creating mod_inventory_storage triggers")


def drop_inventory_triggers(**kwargs):
    """
    Drops Inventory triggers used for optimistic locking
    """
    pg_hook = _db_connection(**kwargs)
    sql = """
          DROP TRIGGER set_instance_ol_version_trigger ON sul_mod_inventory_storage.instance;
          DROP TRIGGER set_holdings_record_ol_version_trigger ON sul_mod_inventory_storage.holdings_record;
          DROP TRIGGER set_item_ol_version_trigger ON sul_mod_inventory_storage.item;
          """
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    logger.info("Finished dropping mod_inventory_storage triggers")


marc_to_db_init = """
CREATE TABLE Instance (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    uuid VARCHAR NOT NULL UNIQUE,
    version INTEGER NOT NULL,
    srs VARCHAR
);

CREATE TABLE SRSUpdate (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    updated_on DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
    instance INTEGER NOT NULL,
    http_status INTEGER NOT NULL,
    http_error VARCHAR,

    FOREIGN KEY (instance) REFERENCES Instance (id)
);
"""


def initilize_marc_to_instances_db(**kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]
    batches = kwargs["batches"]

    iteration_dir = Path(f"{airflow}/migration/marc2instances/{dag.run_id}")

    iteration_dir.mkdir(parents=True, exist_ok=True)

    for batch_number in batches:
        db_path = iteration_dir / f"results-{batch_number}.db"

        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.executescript(marc_to_db_init)
        cur.close()
        con.close()

    return str(iteration_dir.absolute())
