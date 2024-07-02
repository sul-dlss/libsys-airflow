from airflow.models import Connection
from psycopg2.pool import SimpleConnectionPool


class SQLPool:
    def __init__(self, **kwargs):
        self.conn = self.connection()

    def connection(self):
        return Connection.get_connection_from_secrets('postgres_folio')

    def pool(self):
        SimpleConnectionPool(
            12,
            144,
            database='okapi',
            host=self.conn.host,
            password=self.conn.conn_id,
            port=self.conn.port,
            user='okapi',
        )
