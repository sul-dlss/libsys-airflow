import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def drop_inventory_indices(**kwargs):
    postgres_connect = kwargs.get("connection", "postgres_folio")
    database = kwargs.get("database", "okapi")
    index_result = kwargs["index_result"]
    pg_hook = PostgresHook(postgres_conn_id=postgres_connect, database=database)
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

    
def query_inventory_indices(**kwargs):
    postgres_connect = kwargs.get("connection", "postgres_folio")
    database = kwargs.get("database", "okapi")
    pg_hook = PostgresHook(postgres_conn_id=postgres_connect, database=database)
    sql="""
        SELECT indexname, indexdef FROM pg_indexes
        WHERE schemaname = 'sul_mod_inventory_storage' AND
        (tablename = 'instance' OR tablename = 'holdings_records'
        OR tablename = 'item');
        """
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    indices_info = cursor.fetchall()
    return indices_info

