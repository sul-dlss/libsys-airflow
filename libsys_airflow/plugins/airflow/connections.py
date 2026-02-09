import logging
from urllib.parse import urlparse
from typing import Optional
import json

from airflow.sdk import Connection
from airflow.sdk import task
from sqlalchemy.orm.session import Session
from airflow.utils.session import NEW_SESSION, provide_session

from libsys_airflow.plugins.folio.interface import interface_info

logger = logging.getLogger(__name__)

CONN_TYPES = ["ftp", "sftp"]


@task
def create_connection_task(interface_id: str) -> str:
    return create_connection(interface_id)


def create_connection(interface_id: str) -> str:
    info = interface_info(interface_id=interface_id)
    uri = urlparse(info["uri"])
    if uri.scheme not in CONN_TYPES:
        raise ValueError(f"Unknown connection type for {interface_id}: {uri.scheme}")

    conn_id = find_or_create_conn(
        uri.scheme,
        uri.hostname,
        info["username"],
        info["password"],
        uri.port,
        _key_file(info, uri),
    )
    logger.info(f"Connection id for Interface {interface_id}: {conn_id}")
    return conn_id


@provide_session
def find_or_create_conn(
    conn_type: str,
    host: str,
    login: str,
    pwd: Optional[str] = None,
    port: Optional[int] = None,
    key_file: Optional[str] = None,
    session: Session = NEW_SESSION,
) -> str:
    conn_id = f"{conn_type}-{host}-{login}"
    conn = session.query(Connection).filter_by(conn_id=conn_id).first()
    if not conn:
        conn = Connection(conn_id=conn_id)
    conn.conn_id = conn_id
    conn.conn_type = conn_type
    conn.host = host
    conn.login = login
    conn.port = port

    if pwd:
        conn.set_password(pwd)
    if key_file:
        conn.set_extra(json.dumps({"key_file": key_file}))

    session.add(conn)
    session.commit()

    return conn_id


def _key_file(info, uri):
    if info["password"]:
        return None

    # By convention, using the hostname as the key file name
    return f"/opt/airflow/vendor-keys/{uri.hostname}"
