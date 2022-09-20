import json

import pytest  # noqa
import requests

from pytest_mock import MockerFixture
from plugins.tests.mocks import mock_file_system, MockFOLIOClient  # noqa


from plugins.folio.items import (
    post_folio_items_records,
    run_items_transformer,
    _add_additional_info,
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


def test_add_additional_info(mock_file_system, mock_item_note_type):  # noqa
    airflow_path = mock_file_system[0]
    results_dir = mock_file_system[3]
    holdings_path = results_dir / "holdings_transformer-test_dag.json"

    holdings_rec = {
        "id": "8e6e9fb5-f914-4d38-87d2-ccb52f9a44a4",
        "formerIds": ["a23456"],
        "hrid": "ah23456_1",
    }

    holdings_path.write_text(f"{json.dumps(holdings_rec)}\n")

    items_path = results_dir / "items_transformer-test_dag.json"

    items_rec = {
        "holdingsRecordId": "8e6e9fb5-f914-4d38-87d2-ccb52f9a44a4",
        "barcode": "55678446243",
    }

    items_path.write_text(f"{json.dumps(items_rec)}\n")

    data_prep = airflow_path / "migration/data_preparation/"

    data_prep.mkdir(parents=True)

    items_notes_path = data_prep / "test_dag.notes.tsv"

    items_notes = [
        "BARCODE\tnote\tstaffOnly\tTYPE_NAME\n",
        "1233455\ta note\tTrue\tTech Staff\n",
        "55678446243\tavailable for checkout\tFalse\tPublic",
    ]

    items_notes_path.write_text("".join(items_notes))

    folio_client = MockFOLIOClient()

    _add_additional_info(
        airflow=str(mock_file_system[0]),
        holdings_pattern="holdings_transformer-*.json",
        items_pattern="items_transformer-*.json",
        tsv_notes_path=items_notes_path,
        folio_client=folio_client,
    )

    with items_path.open() as items_fo:
        new_items_rec = json.loads(items_fo.readline())

    assert new_items_rec["hrid"] == "ai23456_1"
    assert new_items_rec["id"] == "f644a96e-8bb0-5d99-897a-6b10589fb4da"
    assert new_items_rec["_version"] == 1
    assert new_items_rec["notes"][0]["staffOnly"] == "False"
    assert new_items_rec["notes"][0]["note"] == "available for checkout"
    assert (
        new_items_rec["notes"][0]["itemNoteTypeId"]
        == "62fd6fcc-5cde-4a74-849a-66e2d77a1f12"
    )
