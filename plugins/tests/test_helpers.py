import logging

import pytest
import pydantic
import requests

from airflow.models import Variable
from pytest_mock import MockerFixture

from plugins.folio.helpers import (
    archive_artifacts,
    get_bib_files,
    post_to_okapi,
    process_records,
    setup_dag_run_folders,
    setup_data_logging,
)

from plugins.tests.mocks import mock_dag_run, mock_file_system  # noqa

# Mock xcom messages dict
messages = {}


# Mock xcoms
def mock_xcom_push(*args, **kwargs):
    key = kwargs["key"]
    value = kwargs["value"]
    messages[key] = value


def mock_xcom_pull(*args, **kwargs):
    task_id = kwargs["task_ids"]
    key = kwargs["key"]
    if task_id in messages:
        if key in messages[task_id]:
            return messages[task_id][key]
    return "unknown"


class MockDagRun(pydantic.BaseModel):
    run_id: str = "manual_2022-09-30T22:03:42"


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = mock_xcom_pull
    xcom_push = mock_xcom_push



def test_archive_artifacts(mock_dag_run, mock_file_system):  # noqa
    dag = mock_dag_run
    airflow_path = mock_file_system[0]
    results_dir = mock_file_system[3]
    archive_dir = mock_file_system[4]
    tmp_dir = mock_file_system[5]

    # Create mock Instance JSON file
    instance_filename = "folio_instances_bibs-transformer.json"
    instance_file = results_dir / instance_filename
    instance_file.write_text("""{ "id":"abcded2345"}""")

    tmp_filename = "temp_file.json"
    tmp_file = tmp_dir / tmp_filename
    tmp_file.write_text("""{ "key":"vaaluue"}""")

    target_file = archive_dir / instance_filename

    assert not archive_dir.exists()

    archive_artifacts(dag_run=dag, airflow=airflow_path, tmp_dir=tmp_dir)

    assert not instance_file.exists()
    assert not tmp_file.exists()
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
        post_response.text = ""

        return post_response

    monkeypatch.setattr(requests, "post", mock_post)


@pytest.mark.output_capturing
def test_post_to_okapi(
    mock_okapi_success, mock_okapi_variable, mock_dag_run, mock_records, caplog  # noqa
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
        post_response.json = lambda: {
            "errors": [
                {
                    "message": "value already exists in table holdings_record: hld100000000027"
                }
            ]
        }

        return post_response

    monkeypatch.setattr(requests, "post", mock_post)


def test_post_to_okapi_failures(
    mock_okapi_failure,
    mock_okapi_variable,
    mock_dag_run,  # noqa
    mock_records,
    mock_file_system,  # noqa
):
    airflow_path = mock_file_system[0]
    migration_results = mock_file_system[3]

    post_to_okapi(
        token="2345asdf",
        dag_run=mock_dag_run,
        records=mock_records,
        endpoint="/instance-storage/batch/synchronous",
        payload_key="instances",
        airflow=airflow_path,
    )

    error_file = (
        migration_results / "errors-instance-storage-422.json"  # noqa
    )
    assert error_file.exists()


def test_get_bib_files():
    context = {
        "params": {
            "record_group": {
                "marc": "sample.mrc",
                "tsv": ["sample.public.tsv", "sample.circ.tsv"],
                "tsv-base": "sample.tsv",
                "tsv-dates": "sample.dates.tsv",
            },
        }
    }

    global messages
    assert len(messages) == 0

    get_bib_files(task_instance=MockTaskInstance(), context=context)

    assert messages["marc-file"].startswith("sample.mrc")
    assert len(messages["tsv-files"]) == 2
    assert messages["tsv-base"].startswith("sample.tsv")
    assert messages["tsv-dates"].startswith("sample.dates.tsv")
    messages


def test_get_bib_files_no_load():
    with pytest.raises(ValueError, match="Missing bib record load"):
        get_bib_files(task_instance=MockTaskInstance(), context={"params": {}})


def test_setup_dag_run_folders(tmp_path):  # noqa
    airflow = tmp_path / "opt/airflow/"

    dag = MockDagRun()

    iteration_directory = setup_dag_run_folders(airflow=airflow, dag_run=dag)

    assert iteration_directory == f"{airflow}/migration/iterations/{dag.run_id}"


@pytest.fixture
def mock_logger_file_handler(monkeypatch, mocker: MockerFixture):
    def mock_file_handler(*args, **kwargs):
        file_handler = mocker.stub(name="file_handler")
        file_handler.addFilter = lambda x: x
        file_handler.setFormatter = lambda x: x
        file_handler.setLevel = lambda x: x
        return file_handler

    monkeypatch.setattr(logging, "FileHandler", mock_file_handler)

def test_process_records(mock_dag_run, mock_file_system):  # noqa
    airflow_path = mock_file_system[0]
    tmp = mock_file_system[5]
    results_dir = mock_file_system[3]

    # mock results file
    results_file = results_dir / "folio_instances_bibs-transformer.json"
    results_file.write_text(
        """{"id": "de09e01a-6d75-4007-b700-c83a475999b1"}
    {"id": "123326dd-9924-498f-9ca3-4fa00dda6c90"}"""
    )

    num_records = process_records(
        prefix="folio_instances",
        out_filename="instances",
        jobs=1,
        dag_run=mock_dag_run,
        airflow=str(airflow_path),
        tmp=str(tmp),
    )

    assert num_records == 2


class MockFolderStructure(pydantic.BaseModel):
    data_issue_file_path = "data-issues-1345.tsv"


class MockTransform(pydantic.BaseModel):
    _log = None
    folder_structure = MockFolderStructure()


def test_setup_data_logging(mock_logger_file_handler):
    transformer = MockTransform()
    assert hasattr(logging.Logger, "data_issues") is False
    assert len(logging.getLogger().handlers) == 5

    setup_data_logging(transformer)
    assert hasattr(logging.Logger, "data_issues")
    assert len(logging.getLogger().handlers) == 6

    # Removes handler otherwise fails subsequent tests
    file_handler = logging.getLogger().handlers[-1]
    logging.getLogger().removeHandler(file_handler)
