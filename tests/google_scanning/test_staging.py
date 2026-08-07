import json

import pytest  # noqa

from libsys_airflow.plugins.google_scanning.staging import (
    STATUS_SHIPPED,
    STATUS_STAGED,
    STATUS_UNKNOWN,
    list_shipped_carts,
    list_staged_carts,
    save_staged_file,
    shipped_cart_status,
    staged_cart_status,
    trigger_on_campus_shipment_dag,
    trigger_stage_cart_items_dag,
)


@pytest.fixture(autouse=True)
def mock_staged_files_base(tmp_path, mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.STAGED_FILES_BASE",
        tmp_path / "staged",
    )
    return tmp_path / "staged"


@pytest.fixture(autouse=True)
def mock_archived_files_base(tmp_path, mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.ARCHIVED_FILES_BASE",
        tmp_path / "archived",
    )
    return tmp_path / "archived"


def test_save_staged_file(mock_staged_files_base):
    staged_path = save_staged_file("cart-1", "barcodes.txt", b"12345\n67890\n")

    assert staged_path == mock_staged_files_base / "cart-1" / "barcodes.txt"
    assert staged_path.read_bytes() == b"12345\n67890\n"


def test_staged_cart_status_defaults_to_unknown(mock_staged_files_base):
    assert staged_cart_status("cart-1") == {"status": STATUS_UNKNOWN}


def test_staged_cart_status_reads_status_file(mock_staged_files_base):
    cart_dir = mock_staged_files_base / "cart-1"
    cart_dir.mkdir(parents=True)
    status = {
        "cart_name": "cart-1",
        "staged_at": "2026-08-04T15:32:10-07:00",
        "total_barcodes": 12,
        "updated": 10,
        "missing_barcodes": ["36105130791697", "36105130791698"],
        "errors": [],
        "status": STATUS_STAGED,
        "shipped_at": None,
        "shipment_dag_run_id": None,
    }
    (cart_dir / "status.json").write_text(json.dumps(status))

    assert staged_cart_status("cart-1") == status


def test_staged_cart_status_handles_bad_json(mock_staged_files_base, caplog):
    cart_dir = mock_staged_files_base / "cart-1"
    cart_dir.mkdir(parents=True)
    (cart_dir / "status.json").write_text("not json")

    assert staged_cart_status("cart-1") == {"status": STATUS_UNKNOWN}
    assert "Could not parse status file" in caplog.text


def test_list_staged_carts_empty_when_missing_base(tmp_path, mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.STAGED_FILES_BASE",
        tmp_path / "does-not-exist",
    )
    assert list_staged_carts() == []


def test_list_staged_carts(mock_staged_files_base):
    save_staged_file("cart-1", "barcodes.txt", b"12345\n")
    save_staged_file("cart-2", "barcodes.txt", b"67890\n")

    carts = list_staged_carts()

    assert len(carts) == 2
    assert {cart["cart_name"] for cart in carts} == {"cart-1", "cart-2"}
    assert all(cart["filename"] == "barcodes.txt" for cart in carts)
    assert all(cart["status"] == {"status": STATUS_UNKNOWN} for cart in carts)


def test_shipped_cart_status_defaults_to_unknown(mock_archived_files_base):
    assert shipped_cart_status("cart-1") == {"status": STATUS_UNKNOWN}


def test_list_shipped_carts_empty_when_missing_base(tmp_path, mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.ARCHIVED_FILES_BASE",
        tmp_path / "does-not-exist",
    )
    assert list_shipped_carts() == []


def test_list_shipped_carts(mock_archived_files_base):
    cart_dir = mock_archived_files_base / "cart-1"
    cart_dir.mkdir(parents=True)
    (cart_dir / "barcodes.txt").write_bytes(b"12345\n")
    status = {
        "cart_name": "cart-1",
        "status": STATUS_SHIPPED,
        "shipped_at": "20260807",
        "shipment_dag_run_id": "run-456",
    }
    (cart_dir / "status.json").write_text(json.dumps(status))

    carts = list_shipped_carts()

    assert carts == [
        {
            "cart_name": "cart-1",
            "filename": "barcodes.txt",
            "shipped_at": "20260807",
            "status": status,
        }
    ]


def test_list_shipped_carts_unknown_status(mock_archived_files_base):
    cart_dir = mock_archived_files_base / "cart-1"
    cart_dir.mkdir(parents=True)
    (cart_dir / "barcodes.txt").write_bytes(b"12345\n")

    carts = list_shipped_carts()

    assert carts == [
        {
            "cart_name": "cart-1",
            "filename": "barcodes.txt",
            "shipped_at": None,
            "status": {"status": STATUS_UNKNOWN},
        }
    ]


def test_trigger_stage_cart_items_dag(mocker):
    mock_api_client = mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.api_client"
    )
    mock_dag_run_api = mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.DagRunApi"
    )
    mock_api_instance = mock_dag_run_api.return_value
    mock_api_instance.trigger_dag_run.return_value.dag_run_id = "run-123"

    dag_run_id = trigger_stage_cart_items_dag("/path/to/barcodes.txt", "cart-1")

    assert dag_run_id == "run-123"
    mock_api_client.return_value.__enter__.assert_called_once()
    mock_api_instance.trigger_dag_run.assert_called_once()
    call_args = mock_api_instance.trigger_dag_run.call_args
    assert call_args[0][0] == "stage_cart_items"
    assert call_args[0][1].conf == {
        "staged_file_path": "/path/to/barcodes.txt",
        "cart_name": "cart-1",
    }


def test_trigger_on_campus_shipment_dag(mocker):
    mocker.patch("libsys_airflow.plugins.google_scanning.staging.api_client")
    mock_dag_run_api = mocker.patch(
        "libsys_airflow.plugins.google_scanning.staging.DagRunApi"
    )
    mock_api_instance = mock_dag_run_api.return_value
    mock_api_instance.trigger_dag_run.return_value.dag_run_id = "run-456"

    selected_carts = [{"cart_name": "cart-1", "filename": "barcodes.txt"}]
    dag_run_id = trigger_on_campus_shipment_dag(
        selected_carts, "staff@example.com", "2026-08-07"
    )

    assert dag_run_id == "run-456"
    call_args = mock_api_instance.trigger_dag_run.call_args
    assert call_args[0][0] == "on_campus_shipment"
    assert call_args[0][1].conf == {
        "selected_carts": selected_carts,
        "user_email": "staff@example.com",
        "shipped_at": "20260807",
    }
