import pytest

from airflow.models import Connection
from airflow.utils.db import merge_conn
from airflow import settings

from libsys_airflow.plugins.airflow.connections import find_or_create_conn


@pytest.fixture
def db_session():
    Session = getattr(settings, "Session", None)
    if Session is None:
        raise RuntimeError("Session must be set before!")
    session = Session()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def delete_connection(conn_id, session):
    conn = find_connection(conn_id, session)
    if conn:
        session.delete(conn)
        session.commit()

def find_connection(conn_id, session):
    conn = session.query(Connection).filter_by(conn_id=conn_id).first()
    return conn

def test_connection_does_not_exist(db_session):
    delete_connection('ftp-example.com', db_session)
    conn_id = find_or_create_conn('ftp', 'example.com', 'user', 'pass')
    assert conn_id == 'ftp-example.com'
    
    conn = find_connection(conn_id, db_session)
    assert conn.conn_id == 'ftp-example.com'
    assert conn.conn_type == 'ftp'
    assert conn.host == 'example.com'
    assert conn.login == 'user'

def test_connection_already_exists(db_session):
    delete_connection('ftp-example.com', db_session)
    prev_conn = Connection(conn_id='ftp-example.com', conn_type='ftp', host='prev.example.com', login='prev-user', password='prev-pass')
    merge_conn(prev_conn, session=db_session)

    conn_id = find_or_create_conn('ftp', 'example.com', 'user', 'pass')
    assert conn_id == 'ftp-example.com'
    conn = find_connection(conn_id, db_session)
    assert conn.conn_id == 'ftp-example.com'
    assert conn.conn_type == 'ftp'
    assert conn.host == 'example.com'
    assert conn.login == 'user'
