import json
import pathlib
import sqlite3

import pytest
import pydantic
import requests

from folio_uuid.folio_uuid import FOLIONamespaces

from pytest_mock import MockerFixture

from mocks import (  # noqa
    mock_dag_run,
    mock_file_system,
    mock_okapi_success,
    MockFOLIOClient,
)

from libsys_airflow.plugins.folio.audit import setup_audit_db
from libsys_airflow.plugins.folio.remediate import add_missing_records, _post_record, start_record_qa


def mock_audit_database(mock_dag_run, mock_file_system):  # noqa
    airflow = mock_file_system[0]

    current_file = pathlib.Path(__file__)
    db_init_file = current_file.parent / "qa.sql"
    mock_db_init_file = airflow / "plugins/folio/qa.sql"
    mock_db_init_file.write_text(db_init_file.read_text())

    setup_audit_db(airflow=airflow, iteration_id=mock_dag_run.run_id)


class MockFolioClient(pydantic.BaseModel):
    okapi_url = "https://okapi-folio.dev.edu"
    password = "abdccde"
    username = "folio_admin"
    okapi_headers = {}


def test_with_no_missing_records(mock_dag_run, mock_file_system, caplog):  # noqa

    mock_audit_database(mock_dag_run, mock_file_system)

    add_missing_records(
        airflow=mock_file_system[0],
        iteration_id="manual_2022-03-05",
        folio_client=MockFOLIOClient(),
    )

    assert "No missing records found!" in caplog.text


def test_with_missing_records(
    mock_file_system, mock_dag_run, mock_okapi_success, caplog  # noqa
):
    # Mock out missing records in audit database

    mock_audit_database(mock_dag_run, mock_file_system)

    mock_db_filepath = mock_file_system[2] / "results/audit-remediation.db"

    con = sqlite3.connect(str(mock_db_filepath))

    cur = con.cursor()

    # Mocks Missing Instance, Holdings, and Items
    for row in [
        (
            {"id": "4154fd58-812f-5a19-926d-ff9f22ca3d39", "hrid": "a4050002"},
            FOLIONamespaces.instances.value,
        ),
        (
            {"id": "29976b12-b1f2-5e81-9b86-83754489e3bf", "hrid": "ah4000005_1"},
            FOLIONamespaces.holdings.value,
        ),
        (
            {"id": "eeb05d4e-ddaa-59c9-9d0e-f6001d0fe9de", "hrid": "ai4050028_1_1"},
            FOLIONamespaces.items.value,
        ),
    ]:
        record, record_type = row
        cur.execute(
            "INSERT INTO Record (uuid, hrid, folio_type) VALUES (?,?,?)",
            (record["id"], record["hrid"], record_type),
        )
        db_id = cur.lastrowid
        cur.execute(
            "INSERT INTO AuditLog (record_id, status) VALUES (?,?);", (db_id, 2)
        )
        cur.execute(
            "INSERT INTO JsonPayload (record_id, payload) VALUES (?,?);",
            (db_id, json.dumps(record)),
        )

    con.commit()
    cur.close()
    con.close()

    add_missing_records(
        airflow=mock_file_system[0],
        iteration_id="manual_2022-03-05",
        folio_client=MockFOLIOClient(),
    )

    assert "Finished adding 1 Instances" in caplog.text
    assert "Finished adding 1 Holdings" in caplog.text
    assert "Finished adding 1 Items" in caplog.text

    # Delete Database File
    mock_db_filepath.unlink()


@pytest.fixture
def mock_okapi_post(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        if args[0].endswith("bad_data"):
            post_response.status_code = 422
            post_response.text = "Item already exists"
        else:
            post_response.status_code = 201
        return post_response

    monkeypatch.setattr(requests, "post", mock_post)


def test_post_record(mock_file_system, mock_dag_run, mock_okapi_post, caplog):  # noqa
    mock_audit_database(mock_dag_run, mock_file_system)

    mock_db_filepath = mock_file_system[2] / "results/audit-remediation.db"

    mock_item = {"id": "7b392689-1cf1-5a99-852f-a036fb51fa81", "hrid": "ai4050043_1_1"}

    con = sqlite3.connect(str(mock_db_filepath))
    cur = con.cursor()
    cur.execute(
        "INSERT INTO Record (uuid, hrid, folio_type) VALUES (?,?,?)",
        (mock_item["id"], mock_item["hrid"], FOLIONamespaces.items.value),
    )
    item_db_id = cur.lastrowid
    con.commit()
    _post_record(
        post_url="http://okapi.edu/items-storage/item",
        db_id=item_db_id,
        con=con,
        okapi_headers={},
    )
    # Tests missing JSON Payload
    assert "Could not retrieve Payload for Record" in caplog.text

    # Tests
    cur.execute(
        "INSERT INTO JsonPayload (record_id, payload) VALUES (?,?);",
        (item_db_id, json.dumps(mock_item)),
    )
    con.commit()
    _post_record(
        post_url="http://okapi.edu/items-storage/bad_data",
        db_id=item_db_id,
        con=con,
        okapi_headers={},
    )

    cur.execute("SELECT * FROM Errors;")
    errors = cur.fetchone()

    assert "Item already exists" in errors[1]
    assert errors[2] == 422
    assert errors[3] == item_db_id

    cur.close()
    con.close()
    mock_db_filepath.unlink()


@pytest.fixture
def mock_get_current_context(monkeypatch, mocker: MockerFixture):
    def mock_get_current_context():
        context = mocker.stub(name="context")
        context.get = lambda arg: {"iteration_id": "manual_2022-03-05"}
        return context

    monkeypatch.setattr(
        "libsys_airflow.plugins.folio.remediate.get_current_context", mock_get_current_context
    )


def test_start_record_qa(mock_get_current_context):  # noqa
    iteration_id = start_record_qa()
    assert iteration_id == "manual_2022-03-05"


@pytest.fixture
def mock_get_current_context_no_iteration(monkeypatch, mocker: MockerFixture):
    def mock_get_current_context():
        context = mocker.stub(name="context")
        context.get = lambda arg: {}
        return context

    monkeypatch.setattr(
        "libsys_airflow.plugins.folio.remediate.get_current_context", mock_get_current_context
    )


def test_start_record_qa_raise_error(mock_get_current_context_no_iteration):

    with pytest.raises(ValueError, match="Iteration ID cannot be None"):
        start_record_qa()
