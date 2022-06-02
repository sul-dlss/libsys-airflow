import json

import pytest  # noqa

from plugins.folio.items import (
    post_folio_items_records,
    run_items_transformer,
    _add_hrid
)


def test_post_folio_items_records():
    assert post_folio_items_records


def test_items_transformers():
    assert run_items_transformer


def test_add_hrid(tmp_path):  # noqa
    holdings_path = tmp_path / "holdings_transformer-test_dag.json"

    holdings_rec = {
        "id": "8e6e9fb5-f914-4d38-87d2-ccb52f9a44a4",
        "formerIds": ["a23456"]
    }

    holdings_path.write_text(f"{json.dumps(holdings_rec)}\n")

    items_path = tmp_path / "items_transformer-test_dag.json"

    items_rec = {
        "holdingsRecordId": "8e6e9fb5-f914-4d38-87d2-ccb52f9a44a4"
    }

    items_path.write_text(f"{json.dumps(items_rec)}\n")

    _add_hrid("https://okapi-endpoint.edu",
              str(holdings_path),
              str(items_path))

    with items_path.open() as items_fo:
        new_items_rec = json.loads(items_fo.readline())

    assert(new_items_rec['hrid']) == "ai23456_1"
    assert(new_items_rec['id']) == "f40ad979-32e8-5f54-bb3d-698c0f611a54"
