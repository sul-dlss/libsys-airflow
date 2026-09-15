import pytest  # noqa

from libsys_airflow.plugins.google_scanning.caiasoft_shipment import (
    barcode_bin_pairs,
    dispatched_shipments,
    sal3_filestamp,
)


def test_dispatched_shipments_filters_by_status():
    manifest = {
        "manifest": [
            {"shipment": "SHIP-1", "shipment_status": "DISPATCH"},
            {"shipment": "SHIP-2", "shipment_status": "LOADING"},
        ]
    }

    result = dispatched_shipments(manifest)

    assert result == [{"shipment": "SHIP-1", "shipment_status": "DISPATCH"}]


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


def test_barcode_bin_pairs_handles_dict_carts():
    shipments = [
        {
            "shipment": "SHIP-1",
            "carts": {
                "1": {"bin": "BIN-1", "items": ["111", "222"]},
                "2": {"bin": "BIN-2", "items": ["333"]},
            },
        }
    ]

    result = barcode_bin_pairs(shipments)

    assert result == [("111", "BIN-1"), ("222", "BIN-1"), ("333", "BIN-2")]


def test_barcode_bin_pairs_empty_shipments():
    assert barcode_bin_pairs([]) == []


def test_barcode_bin_pairs_missing_or_empty_carts():
    assert barcode_bin_pairs([{"shipment": "SHIP-1"}]) == []
    assert barcode_bin_pairs([{"shipment": "SHIP-1", "carts": []}]) == []
    assert barcode_bin_pairs([{"shipment": "SHIP-1", "carts": {}}]) == []


def test_live_manifest_payload():
    """
    A courier manifest as CaiaSoft actually returns it: "DISPATCH" rather
    than "DISPATCHED", and carts as a list for one shipment and as a dict
    keyed by cart number for the others.
    """
    manifest = {
        "success": True,
        "error": "",
        "shipment_count": 3,
        "manifest": [
            {
                "shipment": "GOOGLE-0001",
                "carts": [
                    {
                        "bin": "GB00000001",
                        "items": ["36105000000294", "36105000000377"],
                        "item_count": 2,
                    }
                ],
                "cart_count": 1,
                "shipment_status": "DISPATCH",
                "dispatch_date": "25/08/2026",
            },
            {
                "shipment": "GOOGLE-0002",
                "carts": {
                    "1": {
                        "bin": "GB00000002",
                        "items": ["36105000003181", "36105000003819"],
                        "item_count": 2,
                    }
                },
                "cart_count": 1,
                "shipment_status": "DISPATCH",
                "dispatch_date": "25/08/2026",
            },
            {
                "shipment": "GOOGLE-0003",
                "carts": {
                    "2": {
                        "bin": "GB00000003",
                        "items": ["36105000004387", "36105000004445"],
                        "item_count": 2,
                    }
                },
                "cart_count": 1,
                "shipment_status": "LOADING",
                "dispatch_date": "",
            },
        ],
    }

    shipments = dispatched_shipments(manifest)

    assert [shipment["shipment"] for shipment in shipments] == [
        "GOOGLE-0001",
        "GOOGLE-0002",
    ]
    assert barcode_bin_pairs(shipments) == [
        ("36105000000294", "GB00000001"),
        ("36105000000377", "GB00000001"),
        ("36105000003181", "GB00000002"),
        ("36105000003819", "GB00000002"),
    ]


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
