import json

import pytest

from libsys_airflow.plugins.google_scanning.constants import STATUS_FAILED
from libsys_airflow.plugins.google_scanning.shipment import (
    archive_shipped_cart,
    barcodes_for_shipment,
    mark_carts_failed,
    resolve_instance_ids,
    update_cart_status,
)

FOUND_BARCODE = "36105000000001"
MISSING_BARCODE = "36105000000002"
MULTIPLE_BARCODE = "36105000000003"
ERROR_BARCODE = "36105000000004"
NO_INSTANCE_BARCODE = "36105000000005"

ITEM_ID = "de17bd82-7ba7-4dc7-a4e0-1e28e6f4b5c7"
INSTANCE_ID = "8576f36e-0ab5-4146-9b6b-9f0b84f7fc74"


@pytest.fixture(autouse=True)
def mock_staged_files_base(tmp_path, mocker):
    staged = tmp_path / "staged"
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.shipment.STAGED_FILES_BASE", staged
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.STAGED_FILES_BASE", staged
    )
    return staged


@pytest.fixture(autouse=True)
def mock_archived_files_base(tmp_path, mocker):
    archived = tmp_path / "archived"
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.shipment.ARCHIVED_FILES_BASE",
        archived,
    )
    return archived


def _stage_cart(base, cart_name: str, barcodes: str, status: dict | None = None):
    cart_dir = base / cart_name
    cart_dir.mkdir(parents=True)
    (cart_dir / "barcodes.txt").write_text(barcodes)
    if status is not None:
        (cart_dir / "status.json").write_text(json.dumps(status))
    return cart_dir


def test_barcodes_for_shipment_returns_all_when_no_exclusions(mock_staged_files_base):
    _stage_cart(mock_staged_files_base, "cart-1", "111\n222\n")

    to_ship, skipped = barcodes_for_shipment(
        [{"cart_name": "cart-1", "filename": "barcodes.txt"}]
    )

    assert to_ship == [("111", "cart-1"), ("222", "cart-1")]
    assert skipped == []


def test_barcodes_for_shipment_excludes_missing_and_errored(mock_staged_files_base):
    _stage_cart(
        mock_staged_files_base,
        "cart-1",
        "111\n222\n333\n",
        status={
            "missing_barcodes": ["222"],
            "errors": [{"barcode": "333", "reason": "boom"}],
        },
    )

    to_ship, skipped = barcodes_for_shipment(
        [{"cart_name": "cart-1", "filename": "barcodes.txt"}]
    )

    assert to_ship == [("111", "cart-1")]
    assert skipped == [
        {
            "cart_name": "cart-1",
            "barcodes": [
                {
                    "barcode": "222",
                    "reason": "No FOLIO item found during staging",
                },
                {"barcode": "333", "reason": "boom"},
            ],
        }
    ]


def test_barcodes_for_shipment_defaults_reason_when_error_missing_one(
    mock_staged_files_base,
):
    _stage_cart(
        mock_staged_files_base,
        "cart-1",
        "111\n",
        status={"missing_barcodes": [], "errors": [{"barcode": "111"}]},
    )

    _, skipped = barcodes_for_shipment(
        [{"cart_name": "cart-1", "filename": "barcodes.txt"}]
    )

    assert skipped == [
        {
            "cart_name": "cart-1",
            "barcodes": [{"barcode": "111", "reason": "Unknown error during staging"}],
        }
    ]


def test_barcodes_for_shipment_merges_multiple_carts(mock_staged_files_base):
    _stage_cart(mock_staged_files_base, "cart-1", "111\n")
    _stage_cart(mock_staged_files_base, "cart-2", "222\n")

    to_ship, skipped = barcodes_for_shipment(
        [
            {"cart_name": "cart-1", "filename": "barcodes.txt"},
            {"cart_name": "cart-2", "filename": "barcodes.txt"},
        ]
    )

    assert to_ship == [("111", "cart-1"), ("222", "cart-2")]
    assert skipped == []


def _item(**overrides) -> dict:
    item = {
        "id": ITEM_ID,
        "barcode": FOUND_BARCODE,
        "instanceRecord": {"id": INSTANCE_ID},
    }
    item.update(overrides)
    return item


@pytest.fixture
def mock_folio_client(mocker):
    def mock_get(*args, **kwargs):
        query = kwargs.get("query", "")
        barcode = query.split("==")[-1]
        match barcode:
            case b if b == FOUND_BARCODE:
                return [_item()]
            case b if b == MULTIPLE_BARCODE:
                return [_item(), _item()]
            case b if b == ERROR_BARCODE:
                raise ValueError("500: Server Error")
            case b if b == NO_INSTANCE_BARCODE:
                return [_item(instanceRecord=None)]
            case _:
                return []

    mock_client = mocker.MagicMock()
    mock_client.folio_get = mocker.Mock(side_effect=mock_get)
    return mock_client


def test_resolve_instance_ids_success(mock_folio_client):
    instance_ids, failures = resolve_instance_ids(
        [(FOUND_BARCODE, "cart-1")], mock_folio_client
    )

    assert instance_ids == {FOUND_BARCODE: INSTANCE_ID}
    assert failures == []


def test_resolve_instance_ids_missing_barcode(mock_folio_client):
    instance_ids, failures = resolve_instance_ids(
        [(MISSING_BARCODE, "cart-1")], mock_folio_client
    )

    assert instance_ids == {}
    assert failures == [
        {
            "barcode": MISSING_BARCODE,
            "cart_name": "cart-1",
            "reason": f"missing barcode: {MISSING_BARCODE}",
        }
    ]


def test_resolve_instance_ids_multiple_items(mock_folio_client):
    instance_ids, failures = resolve_instance_ids(
        [(MULTIPLE_BARCODE, "cart-1")], mock_folio_client
    )

    assert instance_ids == {}
    assert (
        failures[0]["reason"] == f"multiple items found for barcode: {MULTIPLE_BARCODE}"
    )


def test_resolve_instance_ids_folio_error(mock_folio_client):
    instance_ids, failures = resolve_instance_ids(
        [(ERROR_BARCODE, "cart-1")], mock_folio_client
    )

    assert instance_ids == {}
    assert "500: Server Error" in failures[0]["reason"]


def test_resolve_instance_ids_no_instance_record(mock_folio_client):
    instance_ids, failures = resolve_instance_ids(
        [(NO_INSTANCE_BARCODE, "cart-1")], mock_folio_client
    )

    assert instance_ids == {}
    assert failures[0]["reason"] == f"item {ITEM_ID} has no instanceRecord"


def test_resolve_instance_ids_mixed_success_and_failure(mock_folio_client):
    instance_ids, failures = resolve_instance_ids(
        [(FOUND_BARCODE, "cart-1"), (MISSING_BARCODE, "cart-2")], mock_folio_client
    )

    assert instance_ids == {FOUND_BARCODE: INSTANCE_ID}
    assert len(failures) == 1
    assert failures[0]["cart_name"] == "cart-2"


def test_update_cart_status_merges_into_existing_file(mock_staged_files_base):
    cart_dir = _stage_cart(
        mock_staged_files_base,
        "cart-1",
        "111\n",
        status={"status": "staged", "total_barcodes": 5},
    )

    update_cart_status(
        "cart-1", mock_staged_files_base, status="shipped", shipped_at="20260810"
    )

    status = json.loads((cart_dir / "status.json").read_text())
    assert status == {
        "status": "shipped",
        "total_barcodes": 5,
        "shipped_at": "20260810",
    }


def test_update_cart_status_creates_file_when_missing(mock_staged_files_base):
    cart_dir = _stage_cart(mock_staged_files_base, "cart-1", "111\n")

    update_cart_status("cart-1", mock_staged_files_base, status="failed")

    status = json.loads((cart_dir / "status.json").read_text())
    assert status == {"status": "failed"}


def test_update_cart_status_handles_bad_json(mock_staged_files_base, caplog):
    cart_dir = _stage_cart(mock_staged_files_base, "cart-1", "111\n")
    (cart_dir / "status.json").write_text("not json")

    update_cart_status("cart-1", mock_staged_files_base, status="failed")

    assert "Could not parse status file" in caplog.text
    status = json.loads((cart_dir / "status.json").read_text())
    assert status == {"status": "failed"}


def test_mark_carts_failed_marks_every_selected_cart(mock_staged_files_base):
    _stage_cart(mock_staged_files_base, "cart-1", "111\n", status={"status": "staged"})
    _stage_cart(mock_staged_files_base, "cart-2", "222\n", status={"status": "staged"})

    mark_carts_failed([{"cart_name": "cart-1"}, {"cart_name": "cart-2"}], "run-123")

    for cart_name in ["cart-1", "cart-2"]:
        status = json.loads(
            (mock_staged_files_base / cart_name / "status.json").read_text()
        )
        assert status["status"] == STATUS_FAILED
        assert status["shipment_dag_run_id"] == "run-123"


def test_archive_shipped_cart_moves_directory(
    mock_staged_files_base, mock_archived_files_base
):
    cart_dir = _stage_cart(
        mock_staged_files_base, "cart-1", "111\n", status={"status": "shipped"}
    )

    dest = archive_shipped_cart("cart-1")

    assert dest == mock_archived_files_base / "cart-1"
    assert not cart_dir.exists()
    assert (dest / "barcodes.txt").read_text() == "111\n"
    assert json.loads((dest / "status.json").read_text()) == {"status": "shipped"}


def test_archive_shipped_cart_suffixes_a_reused_cart_name(
    mock_staged_files_base, mock_archived_files_base
):
    _stage_cart(mock_staged_files_base, "cart-1", "222\n")
    existing_dest = mock_archived_files_base / "cart-1"
    existing_dest.mkdir(parents=True)
    (existing_dest / "barcodes.txt").write_text("old\n")

    dest = archive_shipped_cart("cart-1")

    assert dest == mock_archived_files_base / "cart-1_2"
    assert (dest / "barcodes.txt").read_text() == "222\n"
    # the earlier shipment's archive is untouched
    assert (existing_dest / "barcodes.txt").read_text() == "old\n"


def test_archive_shipped_cart_suffixes_a_third_shipment(
    mock_staged_files_base, mock_archived_files_base
):
    _stage_cart(mock_staged_files_base, "cart-1", "333\n")
    (mock_archived_files_base / "cart-1").mkdir(parents=True)
    (mock_archived_files_base / "cart-1_2").mkdir(parents=True)

    dest = archive_shipped_cart("cart-1")

    assert dest == mock_archived_files_base / "cart-1_3"
