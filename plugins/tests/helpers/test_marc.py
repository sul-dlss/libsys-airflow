import pydantic
import pytest  # noqa

import folio_migration_tools.migration_tasks.batch_poster as batch_poster

from pymarc import Field, MARCWriter, Record

from plugins.folio.helpers.marc import (
    _add_electronic_holdings,
    _extract_856s,
    move_marc_files,
    _move_001_to_035,
    post_marc_to_srs,
    _query_for_relationships,
    remove_srs_json,
)

from plugins.folio.helpers.marc import process as process_marc

from plugins.tests.mocks import (  # noqa
    mock_okapi_success,
    mock_dag_run,
    mock_file_system,
    mock_okapi_variable,
    MockFOLIOClient,
    MockLibraryConfig,
)

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


@pytest.fixture
def mock_get_req_size(monkeypatch):
    def mock_size(response):
        return "150.00MB"

    monkeypatch.setattr(batch_poster, "get_req_size", mock_size)


@pytest.fixture
def srs_file(mock_file_system):  # noqa
    results_dir = mock_file_system[3]

    srs_filepath = results_dir / "test-srs.json"

    srs_filepath.write_text(
        """{ "id": "e9a161b7-3541-54d6-bd1d-e4f2c3a3db79", "rawRecord": { "content": {"leader": "01634pam a2200433 i 4500"}}}"""
    )
    return srs_filepath


class MockFolioClient(pydantic.BaseModel):
    electronic_access_relationships = [
        {"name": "Resource", "id": "db9092dbc9dd"},
        {"name": "Version of resource", "id": "9cd0"},
        {"name": "Related resource", "id": "4add"},
        {"name": "No display constant generated", "id": "bae0"},
        {"name": "No information provided", "id": "f50c90c9"},
    ]


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = mock_xcom_pull
    xcom_push = mock_xcom_push


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


def test_missing_001_to_034(mock_marc_record):
    record = mock_marc_record
    record.remove_fields("001")
    _move_001_to_035(record)
    assert record.get_fields("035") == []


def test_move_001_to_035(mock_marc_record):
    record = mock_marc_record
    _move_001_to_035(record)
    assert record.get_fields("035")[0].get_subfields("a")[0] == "gls_0987654321"  # noqa


def test_move_marc_files(mock_file_system, mock_dag_run):  # noqa
    task_instance = MockTaskInstance()
    airflow_path = mock_file_system[0]
    source_dir = mock_file_system[1]

    # Calls setup to mock out expected iteration
    # setup_dag_run_folders(dag_run=mock_dag_run, airflow=str(airflow_path))

    sample_mrc = source_dir / "sample.mrc"
    with sample_mrc.open("wb+") as fo:
        marc_record = Record()
        marc_record.add_field(
            Field(tag="245", indicators=[" ", " "], subfields=["a", "A Test Title"])
        )
        writer = MARCWriter(fo)
        writer.write(marc_record)

    sample_mhld_mrc = source_dir / "sample-mhld.mrc"
    with sample_mhld_mrc.open("wb+") as mhld_fo:
        marc_record = Record()
        marc_record.add_field(Field(tag="001", data="a123456"))
        marc_record.add_field(
            Field(
                tag="852",
                indicators=[" ", " "],
                subfields=["a", "CSt", "b", "GREEN", "c", "STACKS"],
            )
        )
        writer = MARCWriter(mhld_fo)
        writer.write(marc_record)

    global messages
    messages["bib-files-group"] = {
        "marc-file": str(sample_mrc),
        "mhld-file": str(sample_mhld_mrc),
    }

    move_marc_files(
        task_instance=task_instance,
        airflow=airflow_path,
        source="symphony",
        dag_run=mock_dag_run,
    )  # noqa
    assert not (source_dir / "sample.mrc").exists()
    assert not (source_dir / "sample-mfld.mrc").exists()

    assert (
        airflow_path
        / f"migration/iterations/{mock_dag_run.run_id}/source_data/holdings/sample-mhld.mrc"
    ).exists()

    assert (
        airflow_path
        / f"migration/iterations/{mock_dag_run.run_id}/source_data/instances/sample.mrc"
    ).exists()

    messages = {}


def test_post_marc_to_srs(
    srs_file,
    mock_okapi_success,  # noqa
    mock_dag_run,  # noqa
    mock_file_system,  # noqa
    mock_get_req_size,
    mock_okapi_variable,  # noqa
    caplog,
):
    dag = mock_dag_run

    base_folder = mock_file_system[0] / "migration"

    library_config = MockLibraryConfig(base_folder=str(base_folder))

    post_marc_to_srs(
        dag_run=dag, library_config=library_config, srs_file="test-srs.json"
    )

    assert library_config.iteration_identifier == dag.run_id
    assert "Finished posting MARC json to SRS" in caplog.text


def test_process_marc():
    assert process_marc


def test_query_for_relationships():
    relationships = _query_for_relationships(folio_client=MockFolioClient())
    assert relationships["0"] == "db9092dbc9dd"
    assert relationships["1"] == "9cd0"
    assert relationships["2"] == "4add"
    assert relationships["8"] == "bae0"
    assert relationships["_"] == "f50c90c9"


def test_remove_srs_json(srs_file, mock_file_system, mock_dag_run):  # noqa
    assert srs_file.exists() is True

    remove_srs_json(
        airflow=mock_file_system[0], dag_run=mock_dag_run, srs_filename="test-srs.json"
    )

    assert srs_file.exists() is False
