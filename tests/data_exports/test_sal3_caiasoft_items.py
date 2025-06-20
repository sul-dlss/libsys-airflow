import pydantic
import pytest

from pathlib import Path
from airflow.models import Connection
from libsys_airflow.plugins.data_exports import sal3_items


class MockSQLExecuteQueryOperator(pydantic.BaseModel):
    def execute(self, sql):
        return None


class MockCursor(pydantic.BaseModel):
    def fetchall(self):
        return mock_result_set()

    def execute(self, sql_stmt):
        self


class MockConnection(pydantic.BaseModel):

    def cursor(self):
        return MockCursor()


@pytest.fixture
def mock_airflow_connection():
    return Connection(  # noqa
        conn_id="postgres-folio",
        conn_type="postgres",
        host="example.com",
        password="pass",
        port=9999,
    )


def mock_result_set():
    """
    returns tuples:
    (
        (
            item_uuid,
            holding_id,
            item_barcode,
            item_status,
            item_permanent_location,
            item_temporary_location,
            holdings_permanent_location,
            item_suppressed
        ),
        (...)
    )
    """
    return [
        (
            '619a0be0-e8bf-40c1-b478-37596d60c8fb',
            '88f4c236-bf1d-4ccd-bd00-5910df08f6a2',
            '36105236439712',
            'Available',
            '4859ee2a-36cb-4b53-8b31-695894244f6e',
            '4859ee2a-36cb-4b53-8b31-695894244f6e',
            'dd7b3371-09db-4f7a-9c50-2f9f727a0855',
            'false',
        ),
        (
            'cddf0be5-aad2-4d3d-9773-7366e970caa2',
            '88f4c236-bf1d-4ccd-bd00-5910df08f6a2',
            '36105236439571',
            'Available',
            '4859ee2a-36cb-4b53-8b31-695894244f6e',
            '4859ee2a-36cb-4b53-8b31-695894244f6e',
            '3c389f2e-25ee-47b5-aa39-15ba0547ff0d',
            'false',
        ),
        (
            '8eeeb324-7dd4-4c22-a137-45754c52b9d2'
            '88f4c236-bf1d-4ccd-bd00-5910df08f6a2'
            '36105236439779'
            'Available',
            '4859ee2a-36cb-4b53-8b31-695894244f6e',
            '4859ee2a-36cb-4b53-8b31-695894244f6e',
            '6e902db0-4a93-467d-86b5-f5616934facf',
            'true',
        ),
    ]


@pytest.fixture
def mock_get_current_context(monkeypatch, mocker):
    def _context():
        context = mocker.stub(name="context")
        context.get = lambda *args: {}
        return context

    monkeypatch.setattr(
        'libsys_airflow.plugins.data_exports.sal3_items.get_current_context',
        _context,
    )


def setup_tests(mocker, mock_airflow_connection):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.sql_pool.Connection.get_connection_from_secrets',
        return_value=mock_airflow_connection,
    )
    mocker.patch(
        'libsys_airflow.plugins.data_exports.sal3_items.SQLExecuteQueryOperator',
        return_value=MockSQLExecuteQueryOperator(),
    )
    mocker.patch(
        'libsys_airflow.plugins.data_exports.sal3_items.sal3_items_sql_file',
        return_value='libsys_airflow/plugins/data_exports/sql/sal3_items.sql',
    )


def test_create_sal3_items_view(
    mocker, mock_airflow_connection, mock_get_current_context, caplog
):
    setup_tests(mocker, mock_airflow_connection)

    query = sal3_items.create_sal3_items_view()
    assert "Refreshing materialized view for sal3 items" in caplog.text
    assert query.startswith("DROP MATERIALIZED VIEW IF EXISTS sal3_items;")


def test_sal3_folio_items_and_csv(mocker, mock_airflow_connection, tmp_path):
    setup_tests(mocker, mock_airflow_connection)

    csv_file = Path(
        sal3_items.folio_items_to_csv(airflow=tmp_path, connection=MockConnection())
    )

    assert csv_file.name.startswith('folio_sync')
    assert csv_file.suffix == '.csv'

    with open(csv_file, 'r') as file:
        lines = file.readlines()

    assert len(lines[1].split(',')) == 8
    assert len(lines) == 4
