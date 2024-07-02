import pydantic
import pytest

from airflow.models import Connection
from libsys_airflow.plugins.data_exports import full_dump_marc
from libsys_airflow.plugins.data_exports.marc import exporter
from libsys_airflow.plugins.data_exports import sql_pool

from unittest.mock import MagicMock


def mock_number_of_records(mock_result_set):
    return len(mock_result_set)


def mock_marc_records():
    return ""


@pytest.fixture
def mock_airflow_connection():
    return Connection(
        conn_id="postgres-folio",
        conn_type="postgres",
        host="example.com",
        password="pass",
        port=9999,
    )


def mock_pool():
    mocker = MagicMock()

    def mock_fetchall(*args):
        return mock_result_set()

    def mock_execute(*args, batch_size, offset):
        return None

    def pool(*args):
        pool_mock = mocker.stub()
        mock_conn = mocker.stub()
        mock_cursor = mocker.stub()
        mock_cursor.execute = mock_execute
        mock_cursor.fetchall = mock_fetchall
        mock_conn.cursor = lambda *args: mock_cursor
        pool_mock.getconn = lambda *args: mock_conn
        return pool_mock

    mocker.pool = pool
    return mocker


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
def mock_get_current_context(mocker):
    context = mocker.stub(name="context")
    context.get = lambda arg: {}
    return context


def test_fetch_full_dump(tmp_path, mocker, mock_airflow_connection, caplog):
    mocker.patch.object(exporter, "S3Path")
    mocker.patch('libsys_airflow.plugins.data_exports.marc.exporter.folio_client')
    # mocker.patch(
    #     'libsys_airflow.plugins.data_exports.full_dump_marc.fetch_number_of_records',
    #     return_value=mock_number_of_records(mock_result_set()),
    # )
    mocker.patch.object(full_dump_marc, "SQLPool", mock_pool)
    mocker.patch(
        'libsys_airflow.plugins.data_exports.sql_pool.Connection.get_connection_from_secrets',
        return_value=mock_airflow_connection,
    )

    full_dump_marc.fetch_full_dump_marc(offset=0, batch_size=3)
    assert "Saving 3 marc records to 0_3.mrc in bucket" in caplog.text

    full_dump_marc.fetch_full_dump_marc(offset=3, batch_size=3)
    assert "Saving 3 marc records to 3_6.mrc in bucket" in caplog.text
