import pathlib
import pydantic
import pytest

from pytest_mock import MockerFixture
from unittest.mock import MagicMock

from libsys_airflow.plugins.data_exports.full_dump_ids import (
  fetch_number_of_records,
  fetch_full_dump_ids
)

from airflow.models import Connection
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



class MockAirflowConnection(pydantic.BaseModel):
    conn_id = "postgres_folio"
    host = "http://example.com/"
    login = "okapi_admin"
    password = "1345"
    port = 5432
    get_hook = mock_get_pg_hook

def mock_get_pg_hook(*args, **kwargs):
    mock_hook = MagicMock()
    return


@pytest.fixture
def mock_airflow_connection(monkeypatch):
    def mock_get_connection(*args):
        return MockAirflowConnection()

    monkeypatch.setattr(
        Connection, "get_connection_from_secrets", mock_get_connection
    )


@pytest.fixture
def mock_sql_execute_query_op():
  mock_sql_execute_query_op = MagicMock

  mock_sql_execute_query_op.execute = mock_result_set

  return mock_sql_execute_query_op


def mock_number_of_records(mock_result_set):
  return len(mock_result_set)


def mock_result_set():
  return [
            ['4e66ce0d-4a1d-41dc-8b35-0914df20c7fb'],
            ['fe2e581f-9767-442a-ae3c-a421ac655fe2'],
            ['fde293e9-f4fa-4236-b9db-2cef7d57d5ee'],
            ['189dda6e-fa87-41ee-9672-d39bef3dda20'],
            ['5d278b69-57b7-4dcd-ba18-62dda2c05b20'],
            ['d90e8f77-421f-4ed0-8e46-9563be80eee4'],
            ['3e35d5a8-2f2e-4b67-bf26-5cfbf341e96a'],
            ['5b8f26de-5773-4f21-ae37-7ab036f8d5e1'],
            ['5ec79836-ef37-4388-b1c3-680c8a0b7cac'],
            ['90e08427-6cef-493a-b1ab-11ec3a8f340d'],
            ['3dcd0768-aa4a-46c8-89ce-e952b9a01205'],
            ['b4bbedbb-b478-45bd-b4f0-092cf9b9aefe'],
        ]


@pytest.fixture
def mock_get_current_context(monkeypatch, mocker: MockerFixture):
    def mock_get_current_context():
        context = mocker.stub(name="context")
        context.get = lambda arg: {}
        return context

    monkeypatch.setattr(
        "libsys_airflow.plugins.data_exports.full_dump_ids.get_current_context",
        mock_get_current_context,
    )


def test_fetch_full_dump_ids(
        tmp_path,
        mocker,
        mock_get_current_context,
        mock_sql_execute_query_op,
        mock_airflow_connection
      ):

    mocker.patch(
        'airflow.providers.common.sql.operators.sql',
        return_value=mock_sql_execute_query_op,
    )
    mocker.patch(
       'libsys_airflow.plugins.data_exports.full_dump_ids.fetch_number_of_records',
       return_value=mock_number_of_records(mock_result_set())
    )

    full_dump_ids = fetch_full_dump_ids(batch_size=3)

    file = pathlib.Path(tmp_path)
    print(full_dump_ids)
    # assert file.exists()

    # with file.open('r') as fo:
    #     id_list = list(row for row in csv.reader(fo))

    # assert id_list[0][1] == "['fe2e581f-9767-442a-ae3c-a421ac655fe2']"

