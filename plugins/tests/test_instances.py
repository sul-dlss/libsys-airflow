import pydantic

from plugins.folio.instances import (
    _add_version,
    post_folio_instance_records,
    run_bibs_transformer
)

instances = [{}, {}]


class MockInstances(pydantic.BaseModel):
    values = lambda x: instances  # noqa


class MockBibsTransformer(pydantic.BaseModel):
    instances = MockInstances()


def test_add_version():
    bib_transformer = MockBibsTransformer()
    _add_version(bib_transformer)
    assert bib_transformer.instances.values()[0]["_version"] == 1


def test_post_folio_instance_records():
    assert post_folio_instance_records


def test_run_bibs_transformer():
    assert run_bibs_transformer
