import pydantic
import pytest

from airflow.models import Connection
from libsys_airflow.plugins.data_exports import full_dump_marc
from libsys_airflow.plugins.data_exports.marc import exporter


class MockSQLExecuteQueryOperator(pydantic.BaseModel):
    recreate: bool = False
    from_date: str = "2023-09-01"
    to_date: str = "2025-02-01"
    include_campus: str = "SUL, LAW, HOOVER"

    def execute(self, sql):
        return None


class MockPsycopg2Cursor(pydantic.BaseModel):
    def fetchall(self):
        return [()]

    def execute(self, sql_stmt, params):
        self


class MockPsycopg2Connection(pydantic.BaseModel):

    def cursor(self):
        return MockPsycopg2Cursor()


class MockCursor(pydantic.BaseModel):
    batch_size: int = 0
    offset: int = 0

    def fetchall(self):
        return mock_result_set()[self.offset : self.batch_size + self.offset]

    def execute(self, sql_stmt, params):
        self.batch_size = params[0]
        self.offset = params[1]


class MockConnection(pydantic.BaseModel):

    def cursor(self):
        return MockCursor()


def mock_number_of_records(mock_result_set):
    return len(mock_result_set)


def mock_marc_records():
    return ""


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
    return [
        (
            'e53ba957-8a95-5a5d-a0b6-4e712b3cb9cc',
            {
                "fields": [
                    {
                        "100": {
                            "ind1": "1",
                            "ind2": " ",
                            "subfields": [{"a": "Sage, Joseph."}],
                        }
                    }
                ],
                "leader": "00855nam a2200289   4500",
            },
        ),
        (
            'e53bac58-0efa-5a2a-bffb-fa44e1dd9ded',
            {
                "fields": [
                    {
                        "100": {
                            "ind1": "1",
                            "ind2": " ",
                            "subfields": [{"a": "Morizot, Pierre."}],
                        }
                    }
                ],
                "leader": "02486cam a2200397 a 4500",
            },
        ),
        (
            'e53bad8c-2a0c-58ce-b082-6a66f93ca238',
            {
                "fields": [
                    {
                        "245": {
                            "ind1": "0",
                            "ind2": "0",
                            "subfields": [{"a": "Trade leads"}],
                        }
                    }
                ],
                "leader": "01229cas a2200373 a 450",
            },
        ),
        (
            'c32aeaa2-4740-5a91-a839-38894720a8df',
            {
                "fields": [
                    {
                        "100": {
                            "ind1": "1",
                            "ind2": " ",
                            "subfields": [{"a": "Hommel, Paul."}],
                        }
                    }
                ],
                "leader": "00760cam a2200241 i 450",
            },
        ),
        (
            'd3f5f06a-f5cc-5606-b30e-aa75b1fbbf8s',
            {
                "fields": [
                    {
                        "100": {
                            "ind1": "1",
                            "ind2": " ",
                            "subfields": [{"a": "Chibwe, E. C."}],
                        }
                    }
                ],
                "leader": "00804cam a2200277 i 4500",
            },
        ),
        (
            'd3f5f2b9-be10-5680-bf86-23abc0eb55fe',
            {
                "fields": [
                    {
                        "100": {
                            "ind1": "1",
                            "ind2": " ",
                            "subfields": [{"a": "Betancur, Belisario,"}],
                        }
                    }
                ],
                "leader": "00971cam a2200301 i 4500",
            },
        ),
    ]


@pytest.fixture
def mock_get_current_context_no_recreate(monkeypatch, mocker):
    def _context():
        context = mocker.stub(name="context")
        context.get = lambda *args: {
            "recreate_view": False,
            "from_date": "2023-09-01",
            "to_date": "2025-02-01",
            "marc_file_dir": "marc-files",
        }
        return context

    monkeypatch.setattr(
        'libsys_airflow.plugins.data_exports.full_dump_marc.get_current_context',
        _context,
    )
    monkeypatch.setattr(
        'libsys_airflow.plugins.data_exports.marc.exporter.get_current_context',
        _context,
    )


@pytest.fixture
def mock_get_current_context_recreate(monkeypatch, mocker):
    def _context():
        context = mocker.stub(name="context")
        context.get = lambda *args: {
            "recreate_view": True,
            "from_date": "2023-09-01",
            "to_date": "2025-02-01",
            "include_campus": "SUL, LAW, HOOVER",
        }
        return context

    monkeypatch.setattr(
        'libsys_airflow.plugins.data_exports.full_dump_marc.get_current_context',
        _context,
    )


def setup_recreate_tests(mocker, mock_airflow_connection):
    mocker.patch(
        'libsys_airflow.plugins.data_exports.sql_pool.Connection.get_connection_from_secrets',
        return_value=mock_airflow_connection,
    )
    mocker.patch(
        'libsys_airflow.plugins.data_exports.full_dump_marc.SQLExecuteQueryOperator',
        return_value=MockSQLExecuteQueryOperator(),
    )
    mocker.patch(
        'libsys_airflow.plugins.data_exports.full_dump_marc.psycopg2.connect',
        return_value=MockPsycopg2Connection(),
    )
    mocker.patch(
        'libsys_airflow.plugins.data_exports.full_dump_marc.materialized_view_sql_file',
        return_value='libsys_airflow/plugins/data_exports/sql/materialized_view.sql',
    )
    mocker.patch(
        'libsys_airflow.plugins.data_exports.full_dump_marc.filter_campus_sql_file',
        return_value='libsys_airflow/plugins/data_exports/sql/filter_campus_ids.sql',
    )


def test_fetch_full_dump(
    tmp_path,
    mocker,
    mock_get_current_context_no_recreate,
    mock_airflow_connection,
    caplog,
):
    mocker.patch.object(exporter, "S3Path")
    mocker.patch('libsys_airflow.plugins.data_exports.marc.exporter.folio_client')
    mocker.patch(
        'libsys_airflow.plugins.data_exports.sql_pool.Connection.get_connection_from_secrets',
        return_value=mock_airflow_connection,
    )

    full_dump_marc.fetch_full_dump_marc(
        offset=0, batch_size=3, connection=MockConnection()
    )
    assert "Saving 3 marc records to 0_3.mrc in bucket" in caplog.text

    full_dump_marc.fetch_full_dump_marc(
        offset=3, batch_size=3, connection=MockConnection()
    )
    assert "Saving 3 marc records to 3_6.mrc in bucket" in caplog.text


def test_no_recreate_filter_campus_ids(
    mocker, mock_get_current_context_no_recreate, mock_airflow_connection, caplog
):
    setup_recreate_tests(mocker, mock_airflow_connection)

    query = full_dump_marc.create_campus_filter_view(
        connection=MockPsycopg2Connection()
    )

    if query is None:
        assert True

    assert "Skipping refresh of campus filter view" in caplog.text


def test_no_recreate_materialized_view(
    mocker, mock_get_current_context_no_recreate, mock_airflow_connection, caplog
):
    setup_recreate_tests(mocker, mock_airflow_connection)

    query = full_dump_marc.create_materialized_view()

    if query is None:
        assert True

    assert "Skipping refresh of materialized view" in caplog.text


def test_recreate_materialized_view(
    mocker, mock_get_current_context_recreate, mock_airflow_connection, caplog
):
    setup_recreate_tests(mocker, mock_airflow_connection)

    query = full_dump_marc.create_materialized_view()

    assert query.startswith("DROP MATERIALIZED VIEW IF EXISTS data_export_marc")
    assert (
        "Refreshing materialized view with dates from: 2023-09-01 to: 2025-02-01"
        in caplog.text
    )


def test_recreate_campus_filter_view(
    mocker, mock_get_current_context_recreate, mock_airflow_connection, caplog
):
    setup_recreate_tests(mocker, mock_airflow_connection)

    query = full_dump_marc.create_campus_filter_view(
        connection=MockPsycopg2Connection()
    )

    assert query.startswith("DROP MATERIALIZED VIEW IF EXISTS filter_campus_ids")
    assert (
        "Refreshing view filter with campus codes ('SUL','LAW','HOOVER')" in caplog.text
    )
