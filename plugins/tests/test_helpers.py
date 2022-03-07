import pytest
import pydantic
import requests

from pymarc import Record, Field
from airflow.models import Variable
from pytest_mock import MockerFixture

from plugins.folio.helpers import (
    archive_artifacts,
    move_marc_files_check_csv,
    post_to_okapi,
    process_marc,
    _move_001_to_035,
    tranform_csv_to_tsv,
    process_records
)


# Mock xcom messages dict
messages = {}


# Mock xcom
def mock_xcom_push(*args, **kwargs):
    key = kwargs["key"]
    value = kwargs["value"]
    messages[key] = value


class MockTaskInstance(pydantic.BaseModel):
    xcom_push = mock_xcom_push


@pytest.fixture
def mock_move_marc_files(tmp_path):
    airflow_path = tmp_path / "opt/airflow/"

    # Mock source and target dirs
    source_dir = airflow_path / "symphony"
    source_dir.mkdir(parents=True)

    sample_marc = source_dir / "sample.mrc"
    sample_marc.write_text("sample")

    target_dir = airflow_path / "migration/data/instances/"
    target_dir.mkdir(parents=True)
    return airflow_path


def test_move_marc_files(mock_move_marc_files):
    task_instance = MockTaskInstance()

    move_marc_files_check_csv(task_instance=task_instance, airflow=mock_move_marc_files, source="symphony")  # noqa
    assert not (mock_move_marc_files / "symphony/sample.mrc").exists()
    assert messages["marc_only"]


def test_move_csv_files(tmp_path, mock_move_marc_files):
    task_instance = MockTaskInstance()

    sample_csv = mock_move_marc_files / "symphony/sample.csv"
    sample_csv.write_text("sample")

    move_marc_files_check_csv(task_instance=task_instance, airflow=mock_move_marc_files, source="symphony")  # noqa
    assert messages["marc_only"] is False


@pytest.fixture
def mock_dag_run(mocker: MockerFixture):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual_2022-02-24"
    return dag_run


def test_archive_artifacts(mock_dag_run, tmp_path):
    dag = mock_dag_run
    airflow_path = tmp_path / "opt/airflow/"

    # Mock Results and Archive Directories
    results_dir = airflow_path / "migration/results"
    results_dir.mkdir(parents=True)
    archive_dir = airflow_path / "migration/archive"
    archive_dir.mkdir(parents=True)

    # Create mock Instance JSON file
    instance_filename = f"folio_instances_{dag.run_id}_bibs-transformer.json"
    instance_file = results_dir / instance_filename

    instance_file.write_text("""{ "id":"abcded2345"}""")

    target_file = archive_dir / instance_filename

    archive_artifacts(dag_run=dag, airflow=airflow_path)

    assert not instance_file.exists()
    assert target_file.exists()


@pytest.fixture
def mock_okapi_variable(monkeypatch):
    def mock_get(key):
        return "https://okapi-folio.dev.edu"

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def mock_records():
    return [
        {"id": "de09e01a-6d75-4007-b700-c83a475999b1"},
        {"id": "123326dd-9924-498f-9ca3-4fa00dda6c90"},
    ]


@pytest.fixture
def mock_okapi_success(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 201

        return post_response

    monkeypatch.setattr(requests, "post", mock_post)


@pytest.mark.output_capturing
def test_post_to_okapi(
    mock_okapi_success, mock_okapi_variable, mock_dag_run, mock_records, caplog
):

    post_to_okapi(
        token="2345asdf",
        dag_run=mock_dag_run(),
        records=mock_records,
        endpoint="/instance-storage/batch/synchronous",
        payload_key="instances",
    )

    assert "Result status code 201 for 2 records" in caplog.text


@pytest.fixture
def mock_okapi_failure(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 422
        post_response.text = """{
            "errors" : [ {
                "message" : "value already exists in table holdings_record: hld100000000027"
            } ]
        }"""  # noqa
        return post_response

    monkeypatch.setattr(requests, "post", mock_post)


def test_post_to_okapi_failures(
    mock_okapi_failure,
    mock_okapi_variable,
    mock_dag_run,
    mock_records,
    tmp_path
):
    migration_results = tmp_path / "migration" / "results"

    migration_results.mkdir(parents=True)

    post_to_okapi(
        token="2345asdf",
        dag_run=mock_dag_run,
        records=mock_records,
        endpoint="/instance-storage/batch/synchronous",
        payload_key="instances",
        airflow=tmp_path,
    )

    error_file = (
        migration_results / "errors-instance-storage-422-manual_2022-02-24.json"  # noqa
    )
    assert error_file.exists()


def test_process_marc():
    assert process_marc


@pytest.fixture
def mock_marc_record():
    record = Record()
    field_245 = Field(
        tag="245",
        indicators=["0", "1"],
        subfields=[
            "a", "The pragmatic programmer : ",
            "b", "from journeyman to master /",
            "c", "Andrew Hunt, David Thomas.",
        ],
    )
    field_001_1 = Field(tag="001", data="a123456789")
    field_001_2 = Field(tag="001", data="gls_0987654321")

    record.add_field(field_001_1, field_001_2, field_245)
    return record


def test_move_001_to_035(mock_marc_record):
    record = mock_marc_record
    _move_001_to_035(record)
    assert record.get_fields("035")[0].get_subfields("a")[0] == "gls_0987654321"  # noqa


def test_tranform_csv_to_tsv():
    assert tranform_csv_to_tsv


def test_process_records(mock_dag_run, tmp_path):
    # mock results file
    airflow = tmp_path / "opt/airflow/"
    tmp = tmp_path / "tmp/"
    tmp.mkdir(parents=True)
    results_dir = airflow / "migration/results"
    results_dir.mkdir(parents=True)
    results_file = results_dir / "folio_instances-manual_2022-02-24.json"

    results_file.write_text("""{"id": "de09e01a-6d75-4007-b700-c83a475999b1"}
    {"id": "123326dd-9924-498f-9ca3-4fa00dda6c90"}""")

    num_records = process_records(prefix="folio_instances",
                                  out_filename="instances",
                                  jobs=1,
                                  dag_run=mock_dag_run,
                                  airflow=str(airflow),
                                  tmp=str(tmp))

    assert num_records == 2
