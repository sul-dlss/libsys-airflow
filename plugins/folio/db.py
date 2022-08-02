import logging

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
