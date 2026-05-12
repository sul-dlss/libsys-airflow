from airflow.sdk import Connection, Variable
from psycopg2.pool import SimpleConnectionPool


class SQLPool:
    def __init__(self, conn_id, **kwargs):
        self.conn_id = conn_id
        self.conn = self.connection()

    def connection(self):
        return Connection.get(self.conn_id)

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
