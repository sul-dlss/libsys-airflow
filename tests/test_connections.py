import pytest

from airflow.models import Connection
from airflow.utils.db import merge_conn
from airflow import settings

from libsys_airflow.plugins.airflow.connections import (
    find_or_create_conn,
    create_connection,
)


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
    delete_connection("ftp-example.com", db_session)
    conn_id = find_or_create_conn(
        "ftp", "example.com", "user", "pass", "/opt/airflow/vendor-keys/example.com"
    )
    assert conn_id == "ftp-example.com"

    conn = find_connection(conn_id, db_session)
    assert conn.conn_id == "ftp-example.com"
    assert conn.conn_type == "ftp"
    assert conn.host == "example.com"
    assert conn.login == "user"


def test_connection_already_exists(db_session):
    delete_connection("ftp-example.com", db_session)
    prev_conn = Connection(
        conn_id="ftp-example.com",
        conn_type="ftp",
        host="prev.example.com",
        login="prev-user",
        password="prev-pass",
        extra=None,
    )
    merge_conn(prev_conn, session=db_session)

    conn_id = find_or_create_conn("ftp", "example.com", "user", "pass", 234, None)
    assert conn_id == "ftp-example.com"
    conn = find_connection(conn_id, db_session)
    assert conn.conn_id == "ftp-example.com"
    assert conn.conn_type == "ftp"
    assert conn.host == "example.com"
    assert conn.login == "user"
    assert conn.port == 234
    assert conn.extra is None


def test_create_connection_ftp(mocker):
    mock_find_or_create_conn = mocker.patch(
        "libsys_airflow.plugins.airflow.connections.find_or_create_conn",
        return_value="ftp-example.com",
    )
    mocker.patch(
        "libsys_airflow.plugins.airflow.connections.interface_info",
        return_value={
            "uri": "ftp://example.com",
            "username": "user",
            "password": "pass",
        },
    )
    conn_id = create_connection("1234")
    assert conn_id == "ftp-example.com"
    mock_find_or_create_conn.assert_called_once_with(
        "ftp", "example.com", "user", "pass", None, None
    )


"""
UNUSED per flake8 F811
def test_create_connection_sftp(mocker):
    mock_find_or_create_conn = mocker.patch(
        "libsys_airflow.plugins.airflow.connections.find_or_create_conn",
        return_value="sftp-66.151.8.124",
    )
    mocker.patch(
        "libsys_airflow.plugins.airflow.connections.interface_info",
        return_value={
            "uri": "sftp://66.151.8.124:10006",
            "username": "user",
            "password": "pass",
        },
    )
    conn_id = create_connection("1234")
    assert conn_id == "sftp-66.151.8.124"
    mock_find_or_create_conn.assert_called_once_with(
        "sftp", "66.151.8.124", "user", "pass", 10006, None
    )
"""


def test_create_connection_sftp_with_keyfile(mocker):
    mock_find_or_create_conn = mocker.patch(
        "libsys_airflow.plugins.airflow.connections.find_or_create_conn",
        return_value="sftp-sftp.amalivre.fr",
    )
    mocker.patch(
        "libsys_airflow.plugins.airflow.connections.interface_info",
        return_value={
            "uri": "sftp://sftp.amalivre.fr",
            "username": "user",
            "password": None,
        },
    )
    conn_id = create_connection("1234")
    assert conn_id == "sftp-sftp.amalivre.fr"
    mock_find_or_create_conn.assert_called_once_with(
        "sftp",
        "sftp.amalivre.fr",
        "user",
        None,
        None,
        "/opt/airflow/vendor-keys/sftp.amalivre.fr",
    )


def test_create_connection_sftp(mocker):
    mocker.patch(
        "libsys_airflow.plugins.airflow.connections.interface_info",
        return_value={
            "uri": "xftp://example.com",
            "username": "user",
            "password": "pass",
        },
    )
    with pytest.raises(ValueError):
        create_connection("1234")
