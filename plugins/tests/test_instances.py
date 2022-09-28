import json
import pydantic

from plugins.folio.instances import (
    _adjust_records,
    post_folio_instance_records,
    run_bibs_transformer,
)

from plugins.tests.mocks import mock_file_system  # noqa

instances = [{}, {}]


class MockResultsFile(pydantic.BaseModel):
    name = ""


class MockBibsProcessor(pydantic.BaseModel):
    results_file = MockResultsFile()


class MockBibsTransformer(pydantic.BaseModel):
    processor = MockBibsProcessor()


def test_adjust_records(mock_file_system):  # noqa
    bib_transformer = MockBibsTransformer()
    instances_file = mock_file_system[3] / "folio_srs_instances.json"
    instances_file.write_text(
        """{"id": "3e815a91-8a6e-4bbf-8bd9-cf42f9f789e1", "hrid": "a123456"}
{"id": "123326dd-9924-498f-9ca3-4fa00dda6c90", "hrid": "a98765"}"""
    )
    tsv_dates_file = mock_file_system[3] / "libr.ckeys.001.dates.tsv"
    tsv_dates_file.write_text(
        """CATKEY\tCREATED_DATE\tCATALOGED_DATE
123456\t19900927\t19950710
98765\t20220101\t0""")

    bib_transformer.processor.results_file.name = str(instances_file)

    _adjust_records(bib_transformer, str(tsv_dates_file))

    with instances_file.open() as fo:
        instance_records = [json.loads(row) for row in fo.readlines()]

    assert instance_records[0]["_version"] == 1
    assert instance_records[0]["catalogedDate"] == "1995-07-10"
    assert "catalogedDate" not in instance_records[1]
    assert not tsv_dates_file.exists()


def test_post_folio_instance_records():
    assert post_folio_instance_records


def test_run_bibs_transformer():
    assert run_bibs_transformer
