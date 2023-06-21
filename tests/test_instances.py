import json

import pydantic
import pytest
import requests

from pytest_mock import MockerFixture

from libsys_airflow.plugins.folio.instances import (
    _adjust_records,
    _get_statistical_codes,
    post_folio_instance_records,
    run_bibs_transformer,
)

from mocks import mock_dag_run, mock_file_system, MockFOLIOClient  # noqa


class MockResultsFile(pydantic.BaseModel):
    name = ""


class MockBibsProcessor(pydantic.BaseModel):
    results_file = MockResultsFile()


class MockBibsTransformer(pydantic.BaseModel):
    processor = MockBibsProcessor()


def mock_stat_codes_response():
    return {
        "statisticalCodes": [
            {"code": "LEVEL3", "id": "ae9ce864-f50c-47ce-a5f1-6579f7057fc5"},
            {"code": "MARCIVE", "id": "d3f618e2-9fa9-4623-94ae-1d95d1d66f79"},
            {"code": "E-THESIS", "id": "0f328803-cd6a-47c0-8e76-f3a775d56884"},
        ]
    }


@pytest.fixture
def mock_okapi_requests(monkeypatch, mocker: MockerFixture):
    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get_result")
        get_response.status_code = 200
        get_response.raise_for_status = lambda: {}
        if "statistical-code-types" in args[0]:
            get_response.json = lambda: {
                "statisticalCodeTypes": [{"id": "d558cce0-cb4b-4f28-aad3-c94c7084b2e3"}]
            }
        else:
            get_response.json = mock_stat_codes_response
        return get_response

    monkeypatch.setattr(requests, "get", mock_get)


def test_adjust_records(mock_file_system, mock_dag_run):  # noqa
    instances_file = mock_file_system[3] / "folio_bib_instances.json"
    instances_file.write_text(
        """{"id": "3e815a91-8a6e-4bbf-8bd9-cf42f9f789e1", "hrid": "a123456", "administrativeNotes": ["Identifier(s) from previous system: a123456"]}
{"id": "123326dd-9924-498f-9ca3-4fa00dda6c90", "hrid": "a98765"}
{"id": "6193afd3-d42f-4051-ad56-273f3ae67e53", "hrid": "a347891"}"""
    )
    tsv_dates_file = mock_file_system[3] / "libr.ckeys.001.dates.tsv"
    tsv_dates_file.write_text(
        """CATKEY\tCREATED_DATE\tCATALOGED_DATE
123456\t19900927\t19950710
98765\t20220101\t0
347891\t20230310\t20230321"""
    )

    instance_statuses = {
        "Cataloged": "9634a5ab-9228-4703-baf2-4d12ebc77d56",
        "Uncataloged": "26f5208e-110a-4394-be29-1569a8c84a65",
    }

    base_tsv = mock_file_system[2] / "source_data/items/sample.tsv"

    with base_tsv.open("w+") as fo:
        for line in [
            "CATKEY\tITEM_CAT1\tITEM_CAT2\tCATALOG_SHADOW",
            "a123456\tE-THESIS\tLEVEL3-CAT\t0",
            "a98765\tMARCIVE\t\t1",
            "a347891\tDIGI-SCAN\t\t0",
        ]:
            fo.write(f"{line}\n")

    statistical_code_ids = {
        "LEVEL3-CAT": "ae9ce864-f50c-47ce-a5f1-6579f7057fc5",
        "LEVEL3OCLC": "ae9ce864-f50c-47ce-a5f1-6579f7057fc5",
        "MARCIVE": "d3f618e2-9fa9-4623-94ae-1d95d1d66f79",
        "E-THESIS": "0f328803-cd6a-47c0-8e76-f3a775d56884",
    }

    _adjust_records(
        instances_record_path=instances_file,
        tsv_dates=str(tsv_dates_file),
        instance_statuses=instance_statuses,
        stat_codes=statistical_code_ids,
        base_tsv=base_tsv,
    )

    with instances_file.open() as fo:
        instance_records = [json.loads(row) for row in fo.readlines()]

    assert instance_records[0]["_version"] == 1
    assert instance_records[0]["catalogedDate"] == "1995-07-10"
    assert instance_records[0]["statusId"] == "9634a5ab-9228-4703-baf2-4d12ebc77d56"
    assert instance_records[0]["administrativeNotes"] == []
    assert len(instance_records[0]["statisticalCodeIds"]) == 2
    assert "catalogedDate" not in instance_records[1]
    assert instance_records[1]["statusId"] == "26f5208e-110a-4394-be29-1569a8c84a65"
    assert instance_records[1]["statisticalCodeIds"] == [
        statistical_code_ids["MARCIVE"]
    ]
    assert instance_records[1]["discoverySuppress"] is True
    assert "statisticalCodeIds" not in instance_records[2]
    assert not tsv_dates_file.exists()


def test_get_statistical_codes(mock_okapi_requests):  # noqa
    stat_codes = _get_statistical_codes(MockFOLIOClient())

    assert stat_codes["LEVEL3-CAT"] == "ae9ce864-f50c-47ce-a5f1-6579f7057fc5"


def test_post_folio_instance_records():
    # if the import is successful, this will always pass because the function ref is truthy, hence the typechecker complaint
    assert post_folio_instance_records  # type: ignore


def test_run_bibs_transformer():
    # if the import is successful, this will always pass because the function ref is truthy, hence the typechecker complaint
    assert run_bibs_transformer  # type: ignore
