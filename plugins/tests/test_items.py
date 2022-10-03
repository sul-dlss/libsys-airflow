import json

import pandas as pd
import pytest  # noqa
import requests

from pytest_mock import MockerFixture
from plugins.tests.mocks import mock_dag_run, mock_file_system, MockFOLIOClient  # noqa


from plugins.folio.items import (
    post_folio_items_records,
    run_items_transformer,
    _add_additional_info,
    _generate_item_notes,
)


def test_post_folio_items_records():
    assert post_folio_items_records


def test_items_transformers():
    assert run_items_transformer


item_note_types = {
    "itemNoteTypes": [
        {"id": "8d0a5eca-25de-4391-81a9-236eeefdd20b", "name": "Note"},
        {"id": "e9f6de86-e564-4095-a61a-38c9e0e6b2fc", "name": "Tech Staff"},
        {"id": "62fd6fcc-5cde-4a74-849a-66e2d77a1f12", "name": "Public"},
        {"id": "1d14675c-c163-4502-98f9-961cd3d17ab2", "name": "Circ Staff"},
    ]
}


@pytest.fixture
def mock_item_note_type(monkeypatch, mocker: MockerFixture):  # noqa
    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get_result")
        get_response.status_code = 200
        get_response.json = lambda: item_note_types
        return get_response

    monkeypatch.setattr(requests, "get", mock_get)


holdings_recs = [
    {
        "id": "8e6e9fb5-f914-4d38-87d2-ccb52f9a44a4",
        "formerIds": ["a23456"],
        "hrid": "ah23456_1",
    },
    {
        "hrid": "ah9704208_1",
        "id": "b9cd36d8-a031-5793-b2e8-42042cc2dade",
        "formerIds": ["a9704208"],
    },
]

items_notes = [
    "BARCODE\tnote\tNOTE_TYPE\n",
    "1233455\ta note\tCIRCSTAFF\n",
    "1233455\ta note for the public\tCIRCNOTE\n",
    "55678446243\tavailable for checkout\tPUBLIC\n",
    "55678446243\ttf:green, hbr 9/20/2013\tTECHSTAFF",
]

items_recs = [
    {
        "holdingsRecordId": "8e6e9fb5-f914-4d38-87d2-ccb52f9a44a4",
        "barcode": "55678446243",
    },
    {
        "holdingsRecordId": "b9cd36d8-a031-5793-b2e8-42042cc2dade",
        "barcode": "1233455",
    },
]


def setup_items_holdings(
    results_dir,
    iteration_dir,
    items_recs=items_recs,
    items_notes=items_notes,
    holdings_recs=holdings_recs,
):
    holdings_path = results_dir / "holdings_transformer-test_dag.json"

    with holdings_path.open("w+") as fo:
        for rec in holdings_recs:
            fo.write(f"{json.dumps(rec)}\n")

    items_path = results_dir / "items_transformer-test_dag.json"

    with items_path.open("w+") as fo:
        for rec in items_recs:
            fo.write(f"{json.dumps(rec)}\n")

    data_prep = iteration_dir / "data_preparation/"

    data_prep.mkdir(parents=True)

    items_notes_path = data_prep / "test_dag.notes.tsv"

    items_notes_path.write_text("".join(items_notes))

    return items_path, items_notes_path


def test_add_additional_info(mock_file_system, mock_dag_run, mock_item_note_type):  # noqa
    airflow_path = mock_file_system[0]
    iteration_dir = mock_file_system[2]
    results_dir = mock_file_system[3]

    items_path, items_notes_path = setup_items_holdings(results_dir, iteration_dir)

    folio_client = MockFOLIOClient()

    _add_additional_info(
        airflow=str(airflow_path),
        dag_run_id=mock_dag_run.run_id,
        holdings_pattern="holdings_transformer-*.json",
        items_pattern="items_transformer-*.json",
        tsv_notes_path=items_notes_path,
        folio_client=folio_client,
    )

    with items_path.open() as items_fo:
        new_items_recs = [json.loads(row) for row in items_fo.readlines()]

    assert new_items_recs[0]["hrid"] == "ai23456_1"
    assert new_items_recs[0]["id"] == "f644a96e-8bb0-5d99-897a-6b10589fb4da"
    assert new_items_recs[0]["_version"] == 1
    assert new_items_recs[0]["notes"][0]["staffOnly"] is False
    assert new_items_recs[0]["notes"][0]["note"] == "available for checkout"
    assert (
        new_items_recs[0]["notes"][0]["itemNoteTypeId"]
        == "62fd6fcc-5cde-4a74-849a-66e2d77a1f12"
    )
    assert new_items_recs[0]["notes"][1]["staffOnly"]
    assert (
        new_items_recs[0]["notes"][1]["itemNoteTypeId"]
        == "e9f6de86-e564-4095-a61a-38c9e0e6b2fc"
    )
    assert new_items_recs[1]["hrid"] == "ai9704208_1"
    assert new_items_recs[1]["notes"][0]["staffOnly"]
    assert (
        new_items_recs[1]["notes"][0]["itemNoteTypeId"]
        == "1d14675c-c163-4502-98f9-961cd3d17ab2"
    )
    assert new_items_recs[1]["notes"][1]["staffOnly"]


def test_add_additional_info_missing_barcode(
    mock_file_system, mock_dag_run, mock_item_note_type  # noqa
):
    iteration_dir = mock_file_system[2]
    results_dir = mock_file_system[3]

    items_recs[0].pop("barcode")

    items_path, items_notes_path = setup_items_holdings(
        results_dir, iteration_dir, items_recs
    )

    folio_client = MockFOLIOClient()

    _add_additional_info(
        airflow=str(mock_file_system[0]),
        dag_run_id=mock_dag_run.run_id,
        holdings_pattern="holdings_transformer-*.json",
        items_pattern="items_transformer-*.json",
        tsv_notes_path=items_notes_path,
        folio_client=folio_client,
    )

    with items_path.open() as items_fo:
        new_items_recs = [json.loads(row) for row in items_fo.readlines()]

    assert new_items_recs[0]["hrid"] == "ai23456_1"
    assert new_items_recs[0]["id"] == "e2b679b5-bdb4-541f-a37d-04377be40847"
    assert "barcode" not in new_items_recs[0]


def test_add_additional_info_hoover(mock_file_system, mock_item_note_type):  # noqa
    pass


def test_generate_item_notes_missing_barcode(caplog):  # noqa
    item = {"hrid": "ai123455"}
    tsv_notes_df = pd.DataFrame()
    _generate_item_notes(item, tsv_notes_df, {})

    assert "Item missing barcode, cannot generate notes" in caplog.text
