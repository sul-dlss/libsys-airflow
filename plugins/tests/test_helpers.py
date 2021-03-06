import logging

import numpy as np
import pandas as pd
import pytest
import pydantic
import requests

from pymarc import Record, Field
from airflow.models import Variable
from pytest_mock import MockerFixture

from plugins.folio.helpers import (
    archive_artifacts,
    move_marc_files_check_tsv,
    post_to_okapi,
    process_marc,
    _merge_notes_into_base,
    _move_001_to_035,
    transform_move_tsvs,
    process_records,
    setup_data_logging,
)

from plugins.tests.mocks import mock_file_system  # noqa

# Mock xcom messages dict
messages = {}


# Mock xcom
def mock_xcom_push(*args, **kwargs):
    key = kwargs["key"]
    value = kwargs["value"]
    messages[key] = value


class MockTaskInstance(pydantic.BaseModel):
    xcom_push = mock_xcom_push


def test_move_marc_files(mock_file_system):  # noqa
    task_instance = MockTaskInstance()
    airflow_path = mock_file_system[0]
    source_dir = mock_file_system[1]

    move_marc_files_check_tsv(
        task_instance=task_instance, airflow=airflow_path, source="symphony"
    )  # noqa
    assert not (source_dir / "sample.mrc").exists()
    assert messages["marc_only"]


def test_move_tsv_files(mock_file_system):  # noqa
    task_instance = MockTaskInstance()
    airflow_path = mock_file_system[0]
    source_dir = mock_file_system[1]
    sample_csv = source_dir / "sample.tsv"
    sample_csv.write_text("sample")

    move_marc_files_check_tsv(
        task_instance=task_instance, airflow=airflow_path, source="symphony"
    )  # noqa
    assert messages["marc_only"] is False


@pytest.fixture
def mock_dag_run(mocker: MockerFixture):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual_2022-02-24"
    return dag_run


def test_archive_artifacts(mock_dag_run, mock_file_system):  # noqa
    dag = mock_dag_run
    airflow_path = mock_file_system[0]
    results_dir = mock_file_system[3]
    archive_dir = mock_file_system[4]
    tmp_dir = mock_file_system[5]

    # Create mock Instance JSON file
    instance_filename = f"folio_instances_{dag.run_id}_bibs-transformer.json"
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
            "a",
            "The pragmatic programmer : ",
            "b",
            "from journeyman to master /",
            "c",
            "Andrew Hunt, David Thomas.",
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


def test_missing_001_to_034(mock_marc_record):
    record = mock_marc_record
    record.remove_fields("001")
    _move_001_to_035(record)
    assert record.get_fields("035") == []


def test_transform_move_tsvs(mock_file_system):  # noqa
    airflow_path = mock_file_system[0]
    source_dir = mock_file_system[1]

    # mock sample tsv
    symphony_tsv = source_dir / "sample.tsv"
    symphony_tsv.write_text(
        "CATKEY\tCALL_NUMBER_TYPE\tBARCODE\n123456\tLC 12345\t45677  "
    )
    tsv_directory = airflow_path / "migration/data/items"
    sample_tsv = tsv_directory / "sample.tsv"
    sample_notes_tsv = tsv_directory / "sample.notes.tsv"

    # mock sample CIRCNOTE tsv
    symphony_circnotes_tsv = source_dir / "sample.circnote.tsv"
    symphony_circnotes_tsv.write_text(
        "BARCODE\tCIRCNOTE\n45677 \tpencil marks 7/28/18cc"
    )

    column_transforms = [
        ("CATKEY", lambda x: f"a{x}"),
        ("BARCODE", lambda x: x.strip()),
    ]

    transform_move_tsvs(
        airflow=airflow_path,
        column_transforms=column_transforms,
        source="symphony",
        tsv_stem="sample",
    )

    f = open(sample_tsv, "r")
    assert f.readlines()[1] == "a123456\tLC 12345\t45677\n"
    f.close()

    f_notes = open(sample_notes_tsv, "r")
    assert (
        f_notes.readlines()[1] == "a123456\tLC 12345\t45677\tpencil marks 7/28/18cc\n"
    )
    f_notes.close()


def test_transform_move_tsvs_doesnt_exit(mock_file_system):  # noqa
    airflow_path = mock_file_system[0]

    with pytest.raises(ValueError, match="sample.tsv does not exist for workflow"):
        transform_move_tsvs(airflow=airflow_path, source="symphony", tsv_stem="sample")


def test_merge_notes_into_base():
    base_df = pd.DataFrame(
        [
            {
                "CATKEY": "a1442278",
                "BARCODE": "36105033974929",
                "BASE_CALL_NUMBER": "PQ6407 .A1 1980B",
            },
            {
                "CATKEY": "a13776856",
                "BARCODE": "36105231406765",
                "BASE_CALL_NUMBER": "KGF3055 .M67 2019",
            },
        ]
    )
    notes_df = pd.DataFrame(
        [{"BARCODE": "36105033974929", "CIRCNOTE": "pen marks 6/5/19cc"}]
    )
    base_df = _merge_notes_into_base(base_df, notes_df)
    assert "CIRCNOTE" in base_df.columns

    note_row = base_df.loc[base_df["BARCODE"] == "36105033974929"]
    assert note_row["CIRCNOTE"].item() == "pen marks 6/5/19cc"

    no_note_row = base_df.loc[base_df["BARCODE"] == "36105231406765"]
    assert no_note_row["CIRCNOTE"].item() is np.nan


def test_process_records(mock_dag_run, mock_file_system):  # noqa
    airflow_path = mock_file_system[0]
    tmp = mock_file_system[5]
    results_dir = mock_file_system[3]

    # mock results file
    results_file = results_dir / "folio_instances-manual_2022-02-24.json"
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


@pytest.fixture
def mock_logger_file_handler(monkeypatch, mocker: MockerFixture):
    def mock_file_handler(*args, **kwargs):
        file_handler = mocker.stub(name="file_handler")
        file_handler.addFilter = lambda x: x
        file_handler.setFormatter = lambda x: x
        file_handler.setLevel = lambda x: x
        return file_handler

    monkeypatch.setattr(logging, "FileHandler", mock_file_handler)


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
