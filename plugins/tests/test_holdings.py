import pytest
import pydantic
import requests

from pytest_mock import MockerFixture
from airflow.models import Variable

from plugins.folio.holdings import (
    post_folio_holding_records,
    run_holdings_tranformer,
    _add_hrid,
)


@pytest.fixture
def mock_okapi_success(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 201

        return post_response

    monkeypatch.setattr(requests, "post", mock_post)


@pytest.fixture
def mock_dag_run(mocker: MockerFixture):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual_2022-03-05"
    return dag_run


@pytest.fixture
def mock_okapi_variable(monkeypatch):
    def mock_get(key):
        return "https://okapi-folio.dev.edu"

    monkeypatch.setattr(Variable, "get", mock_get)


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = lambda *args, **kwargs: "a0token"  # noqa


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
    {"id": "abcdedf123345", "instanceId": "xyzabc-def-ha", "formerIds": ["a123345"]}
]


class MockHoldings(pydantic.BaseModel):
    values = lambda *args, **kwargs: holdings  # noqa


class MockMapper(pydantic.BaseModel):
    holdings_hrid_counter: int = 1
    holdings_hrid_prefix: str = "hold"


class MockHoldingsTransformer(pydantic.BaseModel):
    holdings: MockHoldings = MockHoldings()
    mapper: MockMapper = MockMapper()


def test_add_hrid():
    transformer = MockHoldingsTransformer()
    _add_hrid(transformer)

    assert transformer.holdings.values()[0]["hrid"] == "ah123345_1"
