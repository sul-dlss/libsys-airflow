import json
import pathlib
import sqlite3

import psycopg2
import pytest

import pydantic

from airflow.models import Connection

from mocks import mock_dag_run, mock_file_system  # noqa
from libsys_airflow.plugins.folio.audit import (
    _add_json_record,
    audit_instance_views,
    setup_audit_db,
)


class MockCursor(object):
    def __init__(self):
        self.result = "2022-11-03 23:14:29.656514"

    def close(self):
        pass

    def execute(self, *args):
        uuid = args[1]
        if "missing_uuid" == uuid[0]:
            self.result = None

    def fetchone(self):
        return self.result


class MockConnection(pydantic.BaseModel):
    cursor = lambda x: MockCursor()  # noqa: E731
    commit = lambda x: None  # noqa: E731
    close = lambda x: None  # noqa: E731


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


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = lambda *args, **kwargs: "manual_2022-03-05"  # noqa


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


def test_audit_instance_views(
    mock_airflow_connection,
    mock_psycopg2,
    mock_dag_run,  # noqa
    mock_file_system,  # noqa
    caplog,  # noqa
):
    airflow = mock_file_system[0]

    iterations_dir = mock_file_system[2]
    results_dir = iterations_dir / "results"

    current_file = pathlib.Path(__file__)
    db_init_file = current_file.parent.parent / "qa.sql"
    mock_db_init_file = airflow / "qa.sql"
    mock_db_init_file.write_text(db_init_file.read_text())

    audit_db_filepath = results_dir / "audit-remediation.db"

    setup_audit_db(airflow=airflow, iteration_id=mock_dag_run.run_id)

    con = sqlite3.connect(str(audit_db_filepath))
    cur = con.cursor()

    # Adds a Record to test existing ID
    cur.execute(
        "INSERT INTO Record (uuid, hrid) VALUES (?,?);",
        ("89c0646e-03f0-5627-bf12-26313cca6dda", "a4050225"),
    )
    con.commit()

    with (results_dir / "folio_instances_bibs-transformer.json").open("w+") as fo:
        for row in [
            {
                "id": "ed2aaa83-05af-517e-bc6a-fb7eeee9d0d7",
                "hrid": "a4050056",
                "updatedDate": "2022-10-31T20:48:27.492",
                "_version": 1,
            },
            {
                "id": "89c0646e-03f0-5627-bf12-26313cca6dda",
                "hrid": "a4050225",
                "updatedDate": "2022-10-31T20:48:27.641",
                "_version": 1,
            },
        ]:
            fo.write(f"{json.dumps(row)}\n")

    with (results_dir / "folio_holdings.json").open("w+") as fo:
        for row in [
            {
                "id": "e64cb1b4-adaa-59d2-8b0a-97ccc8c86191",
                "hrid": "ah4050006_1",
                "_version": 1,
            },
            {
                "id": "457c5df6-0bcb-5a0b-af2a-072228e0f2dc",
                "hrid": "ah4050013_2",
                "_version": 1,
            },
            {"id": "missing_uuid", "hrid": "ah4400001_1", "_version": 1},
        ]:
            fo.write(f"{json.dumps(row)}\n")

    (results_dir / "folio_items_transformer.json").write_text(
        json.dumps(
            {
                "id": "eeb05d4e-ddaa-59c9-9d0e-f6001d0fe9de",
                "hrid": "ai4050006_1_1",
                "_version": 1,
            }
        )
        + "\n"
    )

    audit_instance_views(
        airflow=str(airflow),
        task_instance=MockTaskInstance(),
    )
    assert "Finished audit of Instance views" in caplog.text

    cur.execute("SELECT count(*) FROM Record;")
    all_records_count = cur.fetchone()[0]
    assert all_records_count == 6

    cur.execute("SELECT count(*) JsonPayload;")

    missing_records_payload = cur.fetchone()[0]
    assert missing_records_payload == 1
    cur.close()
    con.close()
    audit_db_filepath.unlink()


def _init_mock_db(airflow):
    current_file = pathlib.Path(__file__)
    db_init_file = current_file.parent.parent / "qa.sql"
    mock_db_init_file = airflow / "qa.sql"
    mock_db_init_file.write_text(db_init_file.read_text())


def test_setup_audit_db(mock_dag_run, mock_file_system):  # noqa
    airflow = mock_file_system[0]

    iterations_dir = mock_file_system[2]

    _init_mock_db(airflow)

    audit_db_filepath = iterations_dir / "results/audit-remediation.db"

    assert audit_db_filepath.exists() is False

    setup_audit_db(airflow=airflow, iteration_id=mock_dag_run.run_id)

    assert audit_db_filepath.exists()


def test_existing_audit_db(mock_dag_run, mock_file_system, caplog):  # noqa
    airflow = mock_file_system[0]
    iterations_dir = mock_file_system[2]

    audit_db_filepath = iterations_dir / "results/audit-remediation.db"

    audit_db_filepath.write_text("\n")

    setup_audit_db(airflow=airflow, iteration_id=mock_dag_run.run_id)

    assert f"SQLite Database {audit_db_filepath} already exists" in caplog.text


def test_add_json_record_uniqueness(mock_dag_run, mock_file_system, caplog):  # noqa
    airflow = mock_file_system[0]
    iterations_dir = mock_file_system[2]

    audit_db_filepath = iterations_dir / "results/audit-remediation.db"

    _init_mock_db(airflow)

    setup_audit_db(airflow=airflow, iteration_id=mock_dag_run.run_id)

    record = {"id": "f07feb8d-4dbc-408f-a37f-62a53f9d03d4"}
    con = sqlite3.connect(audit_db_filepath)

    cur = con.cursor()
    cur.execute(
        "INSERT INTO Record (uuid, folio_type) VALUES (?,?);", (record["id"], 2)
    )
    con.commit()
    record_id = cur.lastrowid

    cur.execute(
        "INSERT INTO JsonPayload (record_id, payload) VALUES (?,?)",
        (record_id, json.dumps(record)),
    )
    con.commit()

    # Test if record is string and not a dict
    record_two = {"id": "a9420761-7a90-4245-ac95-e8545d65e07f"}
    cur.execute(
        "INSERT INTO Record (uuid, folio_type) VALUES (?,?);", (record_two["id"], 2)
    )
    con.commit()
    record_two_id = cur.lastrowid
    cur.execute(
        "INSERT INTO JsonPayload (record_id, payload) VALUES (?,?)",
        (record_two_id, json.dumps(record_two)),
    )
    cur.close()

    _add_json_record(record, con, record_id)
    _add_json_record(json.dumps(record_two), con, record_two_id)

    # Test if record is list
    _add_json_record([], con, 1)

    assert (
        f"UNIQUE constraint failed: JsonPayload.record_id = {record_id}" in caplog.text
    )
    assert (
        f"UNIQUE constraint failed: JsonPayload.record_id = {record_two_id}"
        in caplog.text
    )
    assert "Unknown record format <class 'list'>" in caplog.text

    con.close()


def test_missing_files(
    mock_airflow_connection,
    mock_psycopg2,
    mock_dag_run,  # noqa
    mock_file_system,  # noqa
    caplog,
):
    airflow = mock_file_system[0]

    _init_mock_db(airflow)

    setup_audit_db(airflow=airflow, iteration_id=mock_dag_run.run_id)

    audit_instance_views(
        airflow=str(airflow),
        task_instance=MockTaskInstance(),
    )

    assert "folio_instances_bibs-transformer.json does not exist." in caplog.text
    assert "folio_holdings.json does not exist." in caplog.text
    assert "folio_items_transformer.json does not exist." in caplog.text
