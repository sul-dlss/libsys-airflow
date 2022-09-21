import logging

import pandas as pd
import pytest
import pydantic
import requests

from pymarc import Record, Field, MARCWriter
from airflow.models import Variable
from pytest_mock import MockerFixture

from plugins.folio.helpers import (
    _add_electronic_holdings,
    archive_artifacts,
    _extract_856s,
    get_bib_files,
    move_marc_files,
    post_to_okapi,
    process_marc,
    _merge_notes,
    _move_001_to_035,
    process_records,
    _query_for_relationships,
    setup_data_logging,
    transform_move_tsvs,
)

from plugins.tests.mocks import mock_file_system  # noqa

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


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = mock_xcom_pull
    xcom_push = mock_xcom_push


def test_move_marc_files(mock_file_system):  # noqa
    task_instance = MockTaskInstance()
    airflow_path = mock_file_system[0]
    source_dir = mock_file_system[1]

    sample_mrc = source_dir / "sample.mrc"
    with sample_mrc.open("wb+") as fo:
        marc_record = Record()
        marc_record.add_field(
            Field(tag="245", indicators=[" ", " "], subfields=["a", "A Test Title"])
        )
        writer = MARCWriter(fo)
        writer.write(marc_record)

    global messages
    messages["bib-files-group"] = {"marc-file": str(sample_mrc)}

    move_marc_files(
        task_instance=task_instance, airflow=airflow_path, source="symphony"
    )  # noqa
    assert not (source_dir / "sample.mrc").exists()
    assert (airflow_path / "migration/data/instances/sample.mrc").exists()
    messages = {}


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
        post_response.text = ""

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


def test_transform_move_tsvs(mock_file_system):  # noqa
    airflow_path = mock_file_system[0]
    source_dir = mock_file_system[1]

    # mock sample tsv
    symphony_tsv = source_dir / "sample.tsv"
    symphony_tsv.write_text(
        "CATKEY\tFORMAT\tCALL_NUMBER_TYPE\tBARCODE\tLIBRARY\tITEM_TYPE\n123456\tMARC\tLC 12345\t45677  \tHOOVER\tNONCIRC"
    )

    symphony_notes_tsv = source_dir / "sample.public.tsv"
    symphony_notes_tsv.write_text("BARCODE\tPUBLIC\n45677 \tAvailable for checkout")

    # mock sample CIRCNOTE tsv
    symphony_circnotes_tsv = source_dir / "sample.circnote.tsv"
    symphony_circnotes_tsv.write_text(
        "BARCODE\tCIRCNOTE\n45677 \tpencil marks 7/28/18cc"
    )

    column_transforms = [
        ("CATKEY", lambda x: f"a{x}"),
        ("BARCODE", lambda x: x.strip()),
    ]

    libraries = ["HOOVER", "HV-ARCHIVE"]
    
    data_prep = airflow_path / "migration/data_preparation/"

    data_prep.mkdir(parents=True)

    # Mocks successful upstream task
    global messages
    messages["bib-files-group"] = {
        "tsv-files": [
            str(symphony_notes_tsv),
            str(symphony_circnotes_tsv),
        ],
        "tsv-base": str(symphony_tsv),
    }

    transform_move_tsvs(
        airflow=airflow_path,
        column_transforms=column_transforms,
        libraries=libraries,
        task_instance=MockTaskInstance(),
        source="symphony",
        tsv_stem="sample",
    )
    tsv_directory = airflow_path / "migration/data/items"
    sample_tsv = tsv_directory / "sample.tsv"

    f = open(sample_tsv, "r")
    assert f.readlines()[1] == "a123456\tMARC\tLC 12345\t45677\tHOOVER\tNONCIRC MARC HOOVER\n"
    f.close()

    sample_notes_tsv = tsv_directory / "sample.notes.tsv"
    f_notes = open(sample_notes_tsv, "r")
    assert (
        f_notes.readlines()[1]
        == "a123456\tMARC\tLC 12345\t45677\tHOOVER\tNONCIRC MARC HOOVER\tAvailable for checkout\tpencil marks 7/28/18cc\n"
    )
    f_notes.close()

    messages = {}


def test_transform_move_tsvs_doesnt_exist(mock_file_system):  # noqa
    airflow_path = mock_file_system[0]

    data_prep = airflow_path / "migration/data_preparation/"

    data_prep.mkdir(parents=True)

    with pytest.raises(FileNotFoundError, match="No such file or directory"):
        transform_move_tsvs(
            airflow=airflow_path,
            source="symphony",
            task_instance=MockTaskInstance(),
            tsv_stem="sample",
        )


def test_merge_notes_circnotes(mock_file_system):  # noqa

    circ_path = mock_file_system[1] / "test.sample2.circnote.tsv"

    circ_notes_df = pd.DataFrame(
        [{"BARCODE": "36105033974929  ", "CIRCNOTE": "pen marks 6/5/19cc"}]
    )

    circ_notes_df.to_csv(circ_path, sep="\t", index=False)

    notes_df = _merge_notes(circ_path)

    note_row = notes_df.loc[notes_df["BARCODE"] == "36105033974929"]
    assert note_row["note"].item() == "pen marks 6/5/19cc"
    assert note_row["NOTE_TYPE"].item() == "CIRCNOTE"


def test_merge_notes_techstaff(mock_file_system):  # noqa
    techstaff_path = mock_file_system[1] / "test.sample2.techstaff.tsv"

    techstaff_df = pd.DataFrame(
        [
            {
                "BARCODE": "36105031890341",
                "TECHSTAFF": "rf:370.4 .J65 no.15 c.3, hbr 6/1/06",
            }
        ]
    )

    techstaff_df.to_csv(techstaff_path, sep="\t", index=False)

    notes_df = _merge_notes(techstaff_path)

    note_row = notes_df.loc[notes_df["BARCODE"] == "36105031890341"]
    assert note_row["note"].item() == "rf:370.4 .J65 no.15 c.3, hbr 6/1/06"
    assert note_row["NOTE_TYPE"].item() == "TECHSTAFF"


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


def test_add_electronic_holdings_skip():
    skip_856_field = Field(
        tag="856",
        indicators=["0", "1"],
        subfields=["z", "table of contents", "u", "http://example.com/"],
    )
    assert _add_electronic_holdings(skip_856_field) is False


def test_add_electronic_holdings():
    field_856 = Field(
        tag="856", indicators=["0", "0"], subfields=["u", "http://example.com/"]
    )
    assert _add_electronic_holdings(field_856) is True


class MockFolioClient(pydantic.BaseModel):
    electronic_access_relationships = [
        {"name": "Resource", "id": "db9092dbc9dd"},
        {"name": "Version of resource", "id": "9cd0"},
        {"name": "Related resource", "id": "4add"},
        {"name": "No display constant generated", "id": "bae0"},
        {"name": "No information provided", "id": "f50c90c9"},
    ]


def test_query_for_relationships():
    relationships = _query_for_relationships(folio_client=MockFolioClient())
    assert relationships["0"] == "db9092dbc9dd"
    assert relationships["1"] == "9cd0"
    assert relationships["2"] == "4add"
    assert relationships["8"] == "bae0"
    assert relationships["_"] == "f50c90c9"


def test_extract_856s():
    catkey = "34456"
    all856_fields = [
        Field(
            tag="856",
            indicators=["0", "1"],
            subfields=[
                "3",
                "Finding Aid",
                "u",
                "https://purl.stanford.edu/123345",
                "x",
                "purchased",
                "x",
                "cz4",
                "x",
                "Provider: Cambridge University Press",
                "y",
                "Access on Campus Only",
                "z",
                "Stanford Use Only",
                "z",
                "Restricted",
            ],
        ),
        Field(
            tag="856",
            indicators=["0", "8"],
            subfields=[
                "u",
                "http://doi.org/34456",
                "y",
                "Public Document All Access",
                "z",
                "World Available",
            ],
        ),
        Field(
            tag="856",
            indicators=["0", "1"],
            subfields=["u", "https://example.doi.org/4566", "3", "sample text"],
        ),
    ]
    output = _extract_856s(
        catkey,
        all856_fields,
        {
            "1": "3b430592-2e09-4b48-9a0c-0636d66b9fb3",
            "_": "f50c90c9-bae0-4add-9cd0-db9092dbc9dd",
        },
    )
    assert len(output) == 2
    assert output[0] == {
        "CATKEY": "34456",
        "HOMELOCATION": "SUL-SDR",
        "LIBRARY": "SUL-SDR",
        "LINK_TEXT": "Access on Campus Only",
        "MAT_SPEC": "Finding Aid",
        "PUBLIC_NOTE": "Stanford Use Only Restricted",
        "RELATIONSHIP": "3b430592-2e09-4b48-9a0c-0636d66b9fb3",
        "URI": "https://purl.stanford.edu/123345",
        "VENDOR_CODE": "cz4",
        "NOTE": "purchased|Provider: Cambridge University Press",
    }
    assert output[1]["LIBRARY"].startswith("SUL")
    assert output[1]["HOMELOCATION"].startswith("INTERNET")
