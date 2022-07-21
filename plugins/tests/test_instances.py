import json
import pydantic

from plugins.folio.instances import (
    _add_version,
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


def test_add_version(mock_file_system):  # noqa
    bib_transformer = MockBibsTransformer()
    instances_file = mock_file_system[3] / "folio_srs_instances.json"
    instances_file.write_text(
        json.dumps({"id": "3e815a91-8a6e-4bbf-8bd9-cf42f9f789e1"})
    )

    bib_transformer.processor.results_file.name = str(instances_file)

    _add_version(bib_transformer)

    with instances_file.open() as fo:
        instance_records = [json.loads(row) for row in fo.readlines()]

    assert instance_records[0]["_version"] == 1


def test_post_folio_instance_records():
    assert post_folio_instance_records


def test_run_bibs_transformer():
    assert run_bibs_transformer
