from airflow.models import Connection, Variable
from psycopg2.pool import SimpleConnectionPool


class SQLPool:
    def __init__(self, **kwargs):
        self.conn = self.connection()

    def connection(self):
        return Connection.get_connection_from_secrets('postgres_folio')

    def pool(self):
        conn = self.conn
        return SimpleConnectionPool(
            12,
            Variable.get('folio_sql_max_pool_size', 48),
            database='okapi',
            host=conn.host,
            password=conn.password,
            port=conn.port,
            user='okapi',
        )
