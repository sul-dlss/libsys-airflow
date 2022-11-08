import json
import pathlib
import sqlite3

import psycopg2
import pytest

import pydantic

from airflow.models import Connection

from plugins.tests.mocks import mock_dag_run, mock_file_system  # noqa
from plugins.folio.audit import audit_instance_views, setup_audit_db


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
    cursor = lambda x: MockCursor()  # noqa
    commit = lambda x: None  # noqa
    close = lambda x: None  # noqa


class MockExtraDeJson(pydantic.BaseModel):
    get = lambda *args: False  # noqa
    items = lambda x: []  # noqa


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
    db_init_file = current_file.parents[2] / "plugins/folio/qa.sql"
    mock_db_init_file = airflow / "plugins/folio/qa.sql"
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

    (results_dir / "folio_holdings_tsv-transformer.json").write_text(
        json.dumps(
            {
                "id": "e64cb1b4-adaa-59d2-8b0a-97ccc8c86191",
                "hrid": "ah4050006_1",
                "_version": 1,
            }
        )
        + "\n"
    )

    (results_dir / "folio_holdings_mhld-transformer.json").write_text(
        json.dumps(
            {
                "id": "457c5df6-0bcb-5a0b-af2a-072228e0f2dc",
                "hrid": "ah4050013_2",
                "_version": 1,
            }
        )
        + "\n"
    )

    (results_dir / "folio_holdings_electronic-transformer.json").write_text(
        json.dumps({"id": "missing_uuid", "hrid": "ah4400001_1", "_version": 1}) + "\n"
    )

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


def test_setup_audit_db(mock_dag_run, mock_file_system):  # noqa
    airflow = mock_file_system[0]

    iterations_dir = mock_file_system[2]

    current_file = pathlib.Path(__file__)
    db_init_file = current_file.parents[2] / "plugins/folio/qa.sql"
    mock_db_init_file = airflow / "plugins/folio/qa.sql"
    mock_db_init_file.write_text(db_init_file.read_text())

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
