import logging

from airflow.models import Connection
from airflow.decorators import task
from sqlalchemy.orm.session import Session
from airflow.utils.session import NEW_SESSION, provide_session

from plugins.folio.interface import interface_info

logger = logging.getLogger(__name__)


@task
def create_connection_task(interface_id: str) -> str:
    info = interface_info(interface_id=interface_id)
    host = info['uri'].removeprefix('ftp://')
    conn_id = find_or_create_conn('ftp', host, info['username'], info['password'])
    logger.info(f"Connection id for Interface {interface_id}: {conn_id}")
    return conn_id


@provide_session
def find_or_create_conn(conn_type: str, host: str, login: str, pwd: str, session: Session = NEW_SESSION) -> str:
    conn_id = f'{conn_type}-{host}'
    conn = Connection(conn_id=conn_id,
                      conn_type=conn_type,
                      host=host,
                      login=login)
    conn.set_password(pwd)
    existing_conn = session.query(conn.__class__).filter_by(conn_id=conn.conn_id).first()

    if existing_conn:
        session.delete(existing_conn)
        session.commit()
    session.add(conn)
    session.commit()

    return conn_id
