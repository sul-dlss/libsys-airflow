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
          FOR EACH ROW sul_mod_inventory_storage.item_set_ol_version();
    """
    logger.info("Creating mod_inventory_storage triggers")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    logger.info("Finished creating mod_inventory_storage triggers")


def add_srs_triggers(**kwargs):
    pg_hook = _db_connection(**kwargs)
    sql = """
          CREATE TRIGGER process_marc_records_lb_insert_update_trigger
          AFTER INSERT OR UPDATE ON sul_mod_source_record_storage.marc_records_lb
          FOR EACH ROW EXECUTE FUNCTION sul_mod_source_record_storage.insert_marc_indexers();
          CREATE TRIGGER update_records_set_leader_record_status
          AFTER INSERT OR DELETE OR UPDATE ON sul_mod_source_record_storage.marc_records_lb
          FOR EACH ROW EXECUTE FUNCTION sul_mod_source_record_storage.update_records_set_leader_record_status();
          """
    logger.info("Creating mod_source_record_storage triggers")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    logger.info("Finished creating mod_source_record_storage triggers")


def drop_inventory_indices(**kwargs):
    pg_hook = _db_connection(**kwargs)
    index_result = kwargs["index_result"]
    sql = ""
    for row in index_result:
        name = row[0]
        if name.endswith("pkey"):
            continue
        sql = f"{sql}DROP INDEX sul_mod_inventory_storage.{name};"
    logger.info("Dropping all mod_inventory_storage indices")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    logger.info("Finished dropping mod_inventory_storage indices")


def drop_inventory_triggers(**kwargs):
    """
    Drops Inventory triggers used for optimistic Locking
    """
    pg_hook = _db_connection(**kwargs)
    sql = """
          DROP TRIGGER set_instance_ol_version_trigger ON sul_mod_inventory_storage.instance;
          DROP TRIGGER set_holdings_record_ol_version_trigger ON sul_mod_inventory_storage.holdings_records;
          DROP TRIGGER set_item_ol_version_trigger ON sul_mod_inventory_storage.item;
          """
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    logger.info("Finished dropping mod_inventory_storage triggers")


def drop_srs_indices(**kwargs):
    pg_hook = _db_connection(**kwargs)
    index_result = kwargs["index_result"]
    sql = ""
    for row in index_result:
        name = row[0]
        sql += f"{sql}DROP INDEX sul_mod_source_record_storage.{name};\n"
    logger.info("Dropping all mod_source_record_storage indices")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    logger.info("Finished dropping mod_source_record_storage indices")


def drop_srs_triggers(**kwargs):
    pg_hook = _db_connection(**kwargs)
    sql = """
          DROP TRIGGER process_marc_records_lb_insert_update_trigger ON sul_mod_source_record_storage.marc_records_lb;
          DROP TRIGGER update_records_set_leader_record_status ON sul_mod_source_record_storage.marc_records_lb;
          """
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    logger.info("Finished dropping mod_source_record_storage triggers")


def query_inventory_indices(**kwargs):
    pg_hook = _db_connection(**kwargs)
    sql = """
        SELECT indexname, indexdef FROM pg_indexes
        WHERE schemaname = 'sul_mod_inventory_storage' AND
        (tablename = 'instance' OR tablename = 'holdings_records'
        OR tablename = 'item');
        """
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    indices_info = cursor.fetchall()
    inventory_indices = []
    for row in indices_info:
        if row[0].endswith("pkey"):
            continue
        inventory_indices.append(row)
    logger.info(f"Finished query for inventory indexes, total {len(inventory_indices)}")
    return inventory_indices


def query_srs_indices(**kwargs):
    pg_hook = _db_connection(**kwargs)
    sql = """
    SELECT indexname, indexdef FROM pg_indexes
    WHERE schemaname = 'sul_mod_source_record_storage';
    """
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    all_srs_indices = cursor.fetchall()
    marc_field_indices = []
    for row in all_srs_indices:
        if row[0].startswith("idx_marc_indexers"):
            marc_field_indices.append(row)
    logger.info(
        f"Finished query for source record storage indexes, total {len(marc_field_indices)}"
    )
    return marc_field_indices
