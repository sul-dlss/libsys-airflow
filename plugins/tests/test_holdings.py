import pytest
import pydantic
import requests

from pytest_mock import MockerFixture
from airflow.models import Variable

from plugins.folio.holdings import (
    post_folio_holding_records,
    run_holdings_tranformer,
    _add_identifiers,
)

from plugins.tests.mocks import (
    mock_okapi_success,
    mock_dag_run,
    mock_okapi_variable,
    MockFOLIOClient,
    MockTaskInstance
)


def test_post_folio_holding_records(
    mock_okapi_success, mock_dag_run, mock_okapi_variable, tmp_path, caplog
):

    dag = mock_dag_run

    holdings_json = tmp_path / f"holdings-{dag.run_id}-1.json"
    holdings_json.write_text(
        """[{ "id": "1233adf" },
    { "id": "45ryry" }]"""
    )

    post_folio_holding_records(
        tmp_dir=tmp_path, task_instance=MockTaskInstance(), dag_run=dag, job=1
    )

    assert "Result status code 201 for 2 records" in caplog.text


def test_run_holdings_tranformer():
    # Waiting until https://github.com/FOLIO-FSE/folio_migration_tools can be
    # installed with pip to test.
    assert run_holdings_tranformer


holdings = [
    {
        "id": "abcdedf123345",
        "instanceId": "xyzabc-def-ha",
        "formerIds": ["a123345"],
        "callNumber": "A1234",
    },
    {
        "id": "exyqdf123345",
        "instanceId": "xyzabc-def-ha",
        "formerIds": ["a123345"],
        "callNumber": "B1234",
    }
]


class MockHoldings(pydantic.BaseModel):
    values = lambda *args, **kwargs: holdings  # noqa




class MockMapper(pydantic.BaseModel):
    # holdings_hrid_counter: int = 1
    # holdings_hrid_prefix: str = "hold"
    folio_client: MockFOLIOClient = MockFOLIOClient()


class MockHoldingsTransformer(pydantic.BaseModel):
    holdings: MockHoldings = MockHoldings()
    mapper: MockMapper = MockMapper()


def test_add_identifiers():
    transformer = MockHoldingsTransformer()
    _add_identifiers(transformer)

    # Test UUIDS
    assert transformer.holdings.values()[0]["id"] == "4a50409a-65de-5581-bfa1-153bc56f57ca"
    assert transformer.holdings.values()[1]["id"] == "0ed484c4-5c2d-5a73-b46d-02b85a56cc3d"

    # Test HRIDs
    assert transformer.holdings.values()[0]["hrid"] == "ah123345_1"
    assert transformer.holdings.values()[1]["hrid"] == "ah123345_2"
