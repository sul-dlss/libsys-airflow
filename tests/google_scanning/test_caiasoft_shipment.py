import pytest  # noqa

from libsys_airflow.plugins.google_scanning.caiasoft_shipment import (
    barcode_bin_pairs,
    dispatched_shipments,
    sal3_filestamp,
)


def test_dispatched_shipments_filters_by_status():
    manifest = {
        "manifest": [
            {"shipment": "SHIP-1", "shipment_status": "DISPATCHED"},
            {"shipment": "SHIP-2", "shipment_status": "LOADING"},
        ]
    }

    result = dispatched_shipments(manifest)

    assert result == [{"shipment": "SHIP-1", "shipment_status": "DISPATCHED"}]


def test_dispatched_shipments_empty_manifest():
    assert dispatched_shipments({}) == []
    assert dispatched_shipments({"manifest": []}) == []


def test_barcode_bin_pairs_flattens_carts_and_items():
    shipments = [
        {
            "shipment": "SHIP-1",
            "carts": [
                {"bin": "BIN-1", "items": ["111", "222"]},
                {"bin": "BIN-2", "items": ["333"]},
            ],
        }
    ]

    result = barcode_bin_pairs(shipments)

    assert result == [("111", "BIN-1"), ("222", "BIN-1"), ("333", "BIN-2")]


def test_barcode_bin_pairs_empty_shipments():
    assert barcode_bin_pairs([]) == []


def test_barcode_bin_pairs_defaults_missing_bin_to_empty_string():
    shipments = [
        {
            "shipment": "SHIP-1",
            "carts": [
                {"items": ["111"]},
                {"bin": "BIN-2", "items": ["222"]},
            ],
        }
    ]

    result = barcode_bin_pairs(shipments)

    assert result == [("111", ""), ("222", "BIN-2")]


def test_sal3_filestamp():
    assert sal3_filestamp("20260813") == "stanford_20260813-sal3"
