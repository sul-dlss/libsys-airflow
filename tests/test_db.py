import psycopg2
import pytest
import pydantic

from airflow.models import Connection


from libsys_airflow.plugins.folio.db import (
    add_inventory_triggers,
    drop_inventory_triggers,
)


class MockCursor(pydantic.BaseModel):
    execute = lambda *args: None  # noqa
    fetchall = lambda x: [  # noqa
        ("idx_marc_indexers_999", "CREATE..."),
        ("another_index", "CREATE..."),
        ("inventory_pkey", "CREATE..."),
    ]


class MockConnection(pydantic.BaseModel):
    cursor = lambda x: MockCursor()  # noqa: E731
    commit = lambda x: None  # noqa: E731


class MockExtraDeJson(pydantic.BaseModel):
    get = lambda *args: False  # noqa: E731

    def items(x) -> list:
        return []


class MockAirflowConnection(pydantic.BaseModel):
    conn_id = 1
    host = "http://example.com/"
    login = "okapi_admin"
    password = "1345"
    port = 5169
    extra_dejson = MockExtraDeJson()


@pytest.fixture
def mock_psycopg2(monkeypatch):
    def mock_connect(*args, **kwargs):
        return MockConnection()

    monkeypatch.setattr(psycopg2, "connect", mock_connect)


@pytest.fixture
def mock_airflow_connection(monkeypatch):
    def mock_get_connection_from_secrets(*args):
        return MockAirflowConnection()

    monkeypatch.setattr(
        Connection, "get_connection_from_secrets", mock_get_connection_from_secrets
    )


def test_add_inventory_triggers(mock_airflow_connection, mock_psycopg2, caplog):
    add_inventory_triggers(connect="folio_postgres", database="okapi")
    assert "Finished creating mod_inventory_storage triggers" in caplog.text


def test_drop_inventory_triggers(mock_airflow_connection, mock_psycopg2, caplog):
    drop_inventory_triggers(connect="folio_postgres", database="okapi")
    assert "Finished dropping mod_inventory_storage triggers" in caplog.text
