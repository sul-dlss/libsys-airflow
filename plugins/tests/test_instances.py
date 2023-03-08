import json
import pydantic

from plugins.folio.instances import (
    _adjust_records,
    post_folio_instance_records,
    run_bibs_transformer,
)

from plugins.tests.mocks import mock_dag_run, mock_file_system  # noqa

instances = [{}, {}]


class MockResultsFile(pydantic.BaseModel):
    name = ""


class MockBibsProcessor(pydantic.BaseModel):
    results_file = MockResultsFile()


class MockBibsTransformer(pydantic.BaseModel):
    processor = MockBibsProcessor()


def test_adjust_records(mock_file_system, mock_dag_run):  # noqa
    instances_file = mock_file_system[3] / "folio_bib_instances.json"
    instances_file.write_text(
        """{"id": "3e815a91-8a6e-4bbf-8bd9-cf42f9f789e1", "hrid": "a123456", "administrativeNotes": ["Identifier(s) from previous system: a123456"]}
{"id": "123326dd-9924-498f-9ca3-4fa00dda6c90", "hrid": "a98765"}"""
    )
    tsv_dates_file = mock_file_system[3] / "libr.ckeys.001.dates.tsv"
    tsv_dates_file.write_text(
        """CATKEY\tCREATED_DATE\tCATALOGED_DATE
123456\t19900927\t19950710
98765\t20220101\t0""")

    instance_statuses = {
        "Cataloged": "9634a5ab-9228-4703-baf2-4d12ebc77d56",
        "Uncataloged": "26f5208e-110a-4394-be29-1569a8c84a65"
    }

    _adjust_records(instances_file, str(tsv_dates_file), instance_statuses)

    with instances_file.open() as fo:
        instance_records = [json.loads(row) for row in fo.readlines()]

    assert instance_records[0]["_version"] == 1
    assert instance_records[0]["catalogedDate"] == "1995-07-10"
    assert instance_records[0]["statusId"] == "9634a5ab-9228-4703-baf2-4d12ebc77d56"
    assert instance_records[0]["administrativeNotes"] == []
    assert "catalogedDate" not in instance_records[1]
    assert instance_records[1]["statusId"] == "26f5208e-110a-4394-be29-1569a8c84a65"
    assert not tsv_dates_file.exists()


def test_post_folio_instance_records():
    assert post_folio_instance_records


def test_run_bibs_transformer():
    assert run_bibs_transformer
