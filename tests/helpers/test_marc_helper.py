import copy
import json
import pathlib
import sqlite3

import requests

import pytest  # noqa

from pytest_mock import MockerFixture
from pymarc import Field, MARCWriter, Record, Subfield
from folio_uuid.folio_uuid import FOLIONamespaces

from libsys_airflow.plugins.folio.audit import setup_audit_db

from libsys_airflow.plugins.folio.helpers.marc import (
    _add_electronic_holdings,
    BatchPoster,
    discover_srs_files,
    _extract_e_holdings_fields,
    filter_mhlds,
    _get_library,
    get_snapshot_id,
    marc_only,
    move_marc_files,
    _move_001_to_035,
    _move_authkeys,
    _move_equals_subfield,
    post_marc_to_srs,
    _remove_unauthorized,
    srs_check_add,
)

from libsys_airflow.plugins.folio.helpers.marc import process as process_marc

from tests.mocks import (  # noqa
    mock_okapi_success,
    mock_dag_run,
    mock_file_system,
    mock_okapi_variable,
    MockFOLIOClient,
    MockLibraryConfig,
    MockTaskInstance,
)

import tests.mocks as mocks


@pytest.fixture
def mock_marc_record():
    record = Record()
    field_245 = Field(
        tag="245",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="a", value="The pragmatic programmer : "),
            Subfield(code="b", value="from journeyman to master /"),
            Subfield(code="c", value="Andrew Hunt, David Thomas."),
        ],
    )
    field_001_1 = Field(tag="001", data="a123456789")
    field_001_2 = Field(tag="001", data="gls_0987654321")

    record.add_field(field_001_1, field_001_2, field_245)
    return record


@pytest.fixture
def mock_srs_requests(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post-result")
        post_response.status_code = 201
        if not args[0].endswith("snapshots"):
            if kwargs["json"].get("id", "") == "c9198b05-8d7e-4769-b0cf-a8ca579c0fb4":
                post_response.status_code = 422
                post_response.text = "Invalid user"
        return post_response

    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get-response")
        if args[0].endswith("e9a161b7-3541-54d6-bd1d-e4f2c3a3db79"):
            get_response.status_code = 200
            get_response.json = lambda: records[0]
        if args[0].endswith("3019a865-f60d-46b9-872e-74d67a1b72d7"):
            get_response.status_code = 200
            get_response.json = lambda: records[-1]
        if args[0].endswith("9cb89c9a-1184-4969-ae0d-19e4667bcea3") or args[0].endswith(
            "c9198b05-8d7e-4769-b0cf-a8ca579c0fb4"
        ):
            get_response.status_code = 404
        if args[0].endswith("d0c4f6ef-770d-44de-9d91-0bc6aa654391"):
            get_response.status_code = 422
            get_response.text = "Missing Required Fields"
        return get_response

    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "get", mock_get)


records = [
    {
        "id": "e9a161b7-3541-54d6-bd1d-e4f2c3a3db79",
        "generation": "0",
        "rawRecord": {"content": {"leader": "01634pam a2200433 i 4500"}},
        "parsedRecord": {"content": {"leader": "01634pam a2200433 i 4500"}},
        "externalIdsHolder": {"instanceHrid": "a34567"},
    },
    {
        "id": "9cb89c9a-1184-4969-ae0d-19e4667bcea3",
        "generation": "0",
        "rawRecord": {"content": {"leader": "01634pam a2200433 i 4500"}},
        "parsedRecord": {"content": {"leader": "01634pam a2200433 i 4500"}},
        "externalIdsHolder": {"instanceHrid": "a13981569"},
    },
    {
        "id": "c9198b05-8d7e-4769-b0cf-a8ca579c0fb4",
        "generation": "0",
        "rawRecord": {"content": {"leader": "01634pam a2200433 i 4500"}},
        "parsedRecord": {"content": {"leader": "01634pam a2200433 i 4500"}},
        "externalIdsHolder": {"instanceHrid": "a165578"},
    },
    {
        "id": "d0c4f6ef-770d-44de-9d91-0bc6aa654391",
        "generation": "0",
        "rawRecord": {"content": {"leader": "01634pam a2200433 i 4500"}},
        "externalIdsHolder": {"instanceHrid": "a11665261"},
    },
    {
        "id": "3019a865-f60d-46b9-872e-74d67a1b72d7",
        "generation": "0",
        "externalIdsHolder": {"instanceHrid": "a8705476"},
    },
]


@pytest.fixture
def srs_file(mock_file_system):  # noqa
    results_dir = mock_file_system[3]

    srs_filepath = results_dir / "folio_srs_instances_bibs-transformer.json"

    with srs_filepath.open("w+") as fo:
        for record in records:
            fo.write(f"{json.dumps(record)}\n")
    return srs_filepath


@pytest.fixture
def mock_batch_poster(monkeypatch):
    def mock_function(*args, **kwargs):
        return

    monkeypatch.setattr(BatchPoster, "__init__", mock_function)
    monkeypatch.setattr(BatchPoster, "do_work", mock_function)
    monkeypatch.setattr(BatchPoster, "wrap_up", mock_function)


def test_add_electronic_holdings_skip():
    skip_856_field = Field(
        tag="856",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="z", value="table of contents"),
            Subfield(code="u", value="http://example.com/"),
        ],
    )
    assert _add_electronic_holdings(skip_856_field) is False


def test_add_electronic_holdings_skip_multiple_fields():
    skip_856_field = Field(
        tag="856",
        indicators=["0", "1"],
        subfields=[
            Subfield(code="z", value="Online Abstract from PCI available"),
            Subfield(code="3", value="More information"),
            Subfield(code="u", value="http://example.com/"),
        ],
    )
    assert _add_electronic_holdings(skip_856_field) is False


def test_add_electronic_holdings():
    field_856 = Field(
        tag="856",
        indicators=["0", "0"],
        subfields=[Subfield(code="u", value="http://example.com/")],
    )
    assert _add_electronic_holdings(field_856) is True


def test_discover_srs_files(mock_file_system, srs_file):  # noqa
    airflow = mock_file_system[0]
    iteration_two_results = airflow / "migration/iterations/manual_2023-03-09/results/"
    iteration_two_results.mkdir(parents=True)

    iterations = discover_srs_files(airflow=mock_file_system[0])

    assert len(iterations) == 1
    assert iterations[0] == str(airflow / "migration/iterations/manual_2022-03-05")


def test_extract_856s():
    catkey = "34456"
    all856_fields = [
        Field(
            tag="856",
            indicators=["0", "1"],
            subfields=[
                Subfield(code="3", value="Finding Aid"),
                Subfield(code="u", value="https://purl.stanford.edu/123345"),
                Subfield(code="x", value="purchased"),
                Subfield(code="x", value="cz4"),
                Subfield(code="x", value="Provider: Cambridge University Press"),
                Subfield(code="y", value="Access on Campus Only"),
                Subfield(code="z", value="Stanford Use Only"),
                Subfield(code="z", value="Restricted"),
            ],
        ),
        Field(
            tag="856",
            indicators=["4", "1"],
            subfields=[
                Subfield(code="u", value="http://purl.stanford.edu/bh752xn6465"),
                Subfield(code="x", value="SDR-PURL"),
                Subfield(
                    code="x",
                    value="file:bh752xn6465%2FSC1228_Powwow_program_2014_001.jp2",
                ),
                Subfield(
                    code="x",
                    value="collection:cy369sj5591:10719939:Stanford University, Native American Cultural Center, records",
                ),
                Subfield(code="x", value="label:2014"),
                Subfield(code="x", value="rights:world"),
            ],
        ),
        Field(
            tag="856",
            indicators=["0", "0"],
            subfields=[
                Subfield(code="u", value="http://doi.org/34456"),
                Subfield(code="y", value="Public Document All Access"),
                Subfield(code="z", value="World Available"),
            ],
        ),
        Field(
            tag="856",
            indicators=["0", "1"],
            subfields=[
                Subfield(code="u", value="https://example.doi.org/4566"),
                Subfield(code="3", value="sample text"),
            ],
        ),
        Field(
            tag="856",
            indicators=["0", "8"],
            subfields=[Subfield(code="u", value="https://example.doi.org/45668")],
        ),
    ]
    output = _extract_e_holdings_fields(
        catkey=catkey, fields=all856_fields, library="SUL"
    )
    assert len(output) == 3
    assert output[0] == {
        "CATKEY": "34456",
        "HOMELOCATION": "SDR",
        "LIBRARY": "SUL",
        "COPY": 0,
        "MAT_SPEC": "Finding Aid",
    }
    assert output[1]["HOMELOCATION"].startswith("SDR")
    assert output[2]["HOMELOCATION"] == "INTERNET"


def test_extract_956s():
    catkey = "a591929"
    all_956_fields = [
        Field(
            tag="956",
            indicators=["4", "1"],
            subfields=[
                Subfield(
                    code="u",
                    value="http://library.stanford.edu/sfx?url%5Fver=Z39.88-2004&9008",
                ),
            ],
        ),
        Field(
            tag="956",
            indicators=["0", "1"],
            subfields=[
                Subfield(
                    code="u",
                    value="http://library.stanford.edu/sfx?url%5Fver=Z39.88-2004&8998",
                ),
                Subfield(code="z", value="Sample text"),
            ],
        ),
    ]

    output = _extract_e_holdings_fields(
        catkey=catkey, fields=all_956_fields, library="SUL"
    )
    assert len(output) == 1
    assert output[0]["HOMELOCATION"].startswith("INTERNET")
    assert output[0]["LIBRARY"] == "SUL"


def test_filter_mhlds(tmp_path, caplog):
    mhld_mock = tmp_path / "mock-mhld.mrc"

    record_one = Record()
    record_one.add_field(
        Field(
            tag="852",
            indicators=[" ", " "],
            subfields=[Subfield(code='a', value='CSt')],
        )
    )
    record_two = Record()
    record_two.add_field(
        Field(
            tag="852",
            indicators=[" ", " "],
            subfields=[Subfield(code='a', value='**REQUIRED Field**')],
        )
    )
    record_three = Record()
    record_three.add_field(
        Field(
            tag="852",
            indicators=[" ", " "],
            subfields=[Subfield(code='z', value='All holdings transferred to CSt')],
        )
    )

    with mhld_mock.open("wb+") as fo:
        marc_writer = MARCWriter(fo)
        for record in [record_one, record_two, record_three]:
            marc_writer.write(record)

    filter_mhlds(mhld_mock)

    assert "Finished filtering MHLD, start 3 removed 2" in caplog.text


def test_get_snapshot_id(mock_srs_requests):
    snapshot = get_snapshot_id(MockFOLIOClient())

    assert len(snapshot) == 36
    assert len(snapshot.split("-")) == 5


def test_marc_only():
    mocks.messages["bib-files-group"] = {
        "tsv-files": [],
        "tsv-base": None,
    }

    next_task = marc_only(
        task_instance=MockTaskInstance(),
        default_task="tsv-holdings",
        marc_only_task="marc-only",
    )

    assert next_task.startswith("marc-only")

    mocks.messages = {}


def test_get_library_default():
    library = _get_library([])
    assert library.startswith("SUL")


def test_get_library_law():
    fields = [Field(tag="596", subfields=[Subfield(code="a", value="24")])]
    library = _get_library(fields)
    assert library.startswith("LAW")


def test_get_library_hoover():
    fields_25 = [Field(tag="596", subfields=[Subfield(code="a", value="25 22")])]
    library_hoover = _get_library(fields_25)
    assert library_hoover.startswith("HOOVER")
    fields_27 = [Field(tag="596", subfields=[Subfield(code="a", value="27 22")])]
    library_hoover2 = _get_library(fields_27)
    assert library_hoover2.startswith("HOOVER")


def test_get_library_business():
    fields = [
        Field(tag="596", subfields=[Subfield(code="a", value="28")]),
        Field(tag="596", subfields=[Subfield(code="a", value="28 22")]),
    ]
    library = _get_library(fields)
    assert library.startswith("BUSINESS")


def test_marc_only_with_tsv():
    mocks.messages["bib-files-group"] = {
        "tsv-files": ["circnotes.tsv"],
        "tsv-base": "base.tsv",
    }

    next_task = marc_only(
        task_instance=MockTaskInstance(),
        default_task="tsv-holdings",
        marc_only_task="marc-only",
    )

    assert next_task.startswith("tsv-holdings")

    mocks.messages = {}


def test_missing_001_to_034(mock_marc_record):
    record = mock_marc_record
    record.remove_fields("001")
    _move_001_to_035(record)
    assert record.get_fields("035") == []


def test_move_001_to_035(mock_marc_record):
    record = mock_marc_record
    _move_001_to_035(record)
    assert (
        record.get_fields("035")[0].get_subfields("a")[0] == "(Sirsi) gls_0987654321"
    )  # noqa


def test_move_authkeys():
    record = Record()
    record.add_field(
        Field(
            tag="240",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="Quintets"),
                Subfield(code="=", value="^A262428"),
            ],
        )
    )
    record.add_field(
        Field(
            tag="245",
            indicators=["1", "0"],
            subfields=[
                Subfield(code="a", value="Forellen-Quintett /"),
                Subfield(code="c", value="Schubert."),
            ],
        )
    )
    record.add_field(
        Field(
            tag="700",
            indicators=["1", " "],
            subfields=[
                Subfield(code="a", value="Haebler, Ingrid,"),
                Subfield(code="d", value="1929-"),
                Subfield(code="e", value="instrumentalist."),
                Subfield(code="=", value="^A856199"),
            ],
        )
    )
    record.add_field(
        Field(
            tag="700",
            indicators=["1", " "],
            subfields=[
                Subfield(code="a", value="Soldiers"),
                Subfield(code="z", value="Pakistan"),
                Subfield(code="=", value="^A1062453"),
            ],
        )
    )
    _move_authkeys(record)
    assert "0" not in record["240"].subfields_as_dict().keys()
    assert "=" not in record["700"].subfields_as_dict().keys()
    fields700 = record.get_fields("700")
    assert fields700[0].get_subfields("0") == ["(SIRSI)856199"]
    assert fields700[1].get_subfields("0") == ["(SIRSI)1062453"]


def test_move_authkeys_240():
    record = Record()
    record.leader[6] = "c"
    record.add_field(
        Field(
            tag="240",
            indicators=[" ", " "],
            subfields=[
                Subfield(code="a", value="Quintets"),
                Subfield(code="=", value="^A262428"),
            ],
        )
    )
    _move_authkeys(record)
    assert "=" not in record["240"].subfields_as_dict().keys()
    assert record["240"].get_subfields("0") == ["(SIRSI)262428"]


def test_move_equals_subfield():
    field_100 = Field(
        tag="100",
        indicators=["1", " "],
        subfields=[
            Subfield(code="a", value="Costa, Robson"),
            Subfield(code="e", value="author."),
            Subfield(code="=", value="^A2387492"),
        ],
    )
    _move_equals_subfield(field_100)
    assert "=" not in field_100.subfields_as_dict().keys()
    assert field_100.get_subfields("0") == ["(SIRSI)2387492"]


def test_move_marc_files(mock_file_system, mock_dag_run):  # noqa
    task_instance = MockTaskInstance()
    airflow_path = mock_file_system[0]
    source_dir = mock_file_system[1]

    sample_mrc = source_dir / "sample.mrc"
    with sample_mrc.open("wb+") as fo:
        marc_record = Record()
        marc_record.add_field(
            Field(
                tag="245",
                indicators=[" ", " "],
                subfields=[Subfield(code="a", value="A Test Title")],
            )
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
                subfields=[
                    Subfield(code="a", value="CSt"),
                    Subfield(code="b", value="GREEN"),
                    Subfield(code="c", value="STACKS"),
                ],
            )
        )
        writer = MARCWriter(mhld_fo)
        writer.write(marc_record)

    mocks.messages["bib-files-group"] = {
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

    mocks.messages = {}


def test_remove_unauthorized(mock_marc_record):
    record = copy.deepcopy(mock_marc_record)
    record.add_field(Field(tag='003', data="SIRSI"))
    record.add_field(
        Field(
            tag='100',
            indicators=["1", " "],
            subfields=[
                Subfield(code="a", value="Ryves, Elizabeth"),
                Subfield(code="?", value="UNAUTHORIZED"),
            ],
        )
    )
    _remove_unauthorized(record)
    assert record['100'].get_subfields("?") == []
    assert "003" in record


def test_post_marc_to_srs(
    srs_file,
    mock_batch_poster,
    mock_dag_run,  # noqa
    mock_file_system,  # noqa
    mock_okapi_variable,  # noqa
    caplog,
):
    airflow = mock_file_system[0]
    dag = mock_dag_run

    base_folder = airflow / "migration"

    library_config = MockLibraryConfig(
        base_folder=str(base_folder), iteration_identifier=mock_dag_run.run_id
    )

    test_srs = base_folder / f"iterations/{mock_dag_run.run_id}/results/test-srs.json"

    test_srs.write_text(json.dumps({}))

    post_marc_to_srs(
        airflow=airflow,
        dag_run=dag,
        library_config=library_config,
        iteration_id="manual_2022-03-05",
        srs_filename="test-srs.json",
    )

    assert library_config.iteration_identifier == dag.run_id
    assert "Finished posting MARC json to SRS" in caplog.text


def test_missing_file_post_marc_to_srs(
    srs_file, mock_dag_run, mock_file_system, caplog  # noqa  # noqa
):
    airflow = mock_file_system[0]

    base_folder = airflow / "migration"

    library_config = MockLibraryConfig(
        base_folder=str(base_folder), iteration_identifier=mock_dag_run.run_id
    )

    post_marc_to_srs(
        airflow=airflow,
        dag_run=mock_dag_run,
        library_config=library_config,
        iteration_id="manual_2022-03-05",
        srs_filename="test-mhlds-srs.json",
    )

    assert "test-mhlds-srs.json does not exist, existing task" in caplog.text


def test_srs_check_add(
    mock_file_system, mock_dag_run, srs_file, mock_srs_requests, caplog  # noqa
):
    airflow = mock_file_system[0]
    results_dir = mock_file_system[3]

    current_file = pathlib.Path(__file__)
    db_init_file = current_file.parent.parent.parent / "qa.sql"
    mock_db_init_file = airflow / "qa.sql"
    mock_db_init_file.write_text(db_init_file.read_text())

    setup_audit_db(airflow=airflow, iteration_id=mock_dag_run.run_id)

    audit_db = sqlite3.connect(results_dir / "audit-remediation.db")

    bib_count = srs_check_add(
        audit_connection=audit_db,
        results_dir=results_dir,
        srs_type=FOLIONamespaces.srs_records_bib.value,
        file_name="folio_srs_instances_bibs-transformer.json",
        snapshot_id="abcdefegrh",
        folio_client=MockFOLIOClient(),
        srs_label="SRS MARC BIBs",
    )

    cur = audit_db.cursor()

    assert bib_count == 5
    existing_records = cur.execute(
        """SELECT count(id) FROM AuditLog WHERE status=1;"""
    ).fetchone()[0]
    assert existing_records == 2
    missing_records = cur.execute(
        """SELECT count(id) FROM AuditLog WHERE status=2;"""
    ).fetchone()[0]
    assert missing_records == 2

    error_records = cur.execute(
        """SELECT count(id) FROM AuditLog WHERE status=3;"""
    ).fetchone()[0]
    assert error_records == 1

    missing_properties_message = cur.execute(
        """SELECT Errors.message FROM Errors, Record
        WHERE Errors.log_id = Record.id AND
        Record.hrid=?;""",
        ('a8705476',),
    ).fetchone()[0]

    assert (
        missing_properties_message
        == "SRS Record missing properties 'parsedRecord' or 'rawRecord' in ['id', 'generation', 'externalIdsHolder']"
    )
    cur.close()


def test_process_marc():
    # if the import is successful, this will always pass because the function ref is truthy, hence the typechecker complaint
    assert process_marc  # type: ignore
