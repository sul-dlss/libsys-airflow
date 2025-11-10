from unittest.mock import MagicMock

import pandas as pd
import pytest

from libsys_airflow.plugins.sdr.helpers import (
    check_update_item,
    delete_barcode_csv,
    extract_barcodes,
    stat_codes_lookup,
)

DIGI_DF = pd.DataFrame(
    [
        {"barcode": "111", "druid": "cb432dm2566"},
        {"barcode": "222", "druid": "tz846ct1052"},
        {"barcode": "333", "druid": "yy824hg8371"},
    ]
)

STAT_CODE_LOOKUP = {
    "ADD": ["af75fbdf-37f1-4d4c-a6ca-0b6ce0539342"],
    "REMOVE": [
        "38c2a160-9a46-4198-a5cb-8517fb7c2cee",
        "4d0ff7ad-cd0f-4d1a-8683-d63162e41b04",
    ],
}


def test_check_update_item_not_found(mocker):
    """Test when no item is found with the barcode"""
    mock_client = MagicMock()
    mock_client.folio_get.return_value = []

    result = check_update_item("12345", mock_client, STAT_CODE_LOOKUP)

    assert result["error"].startswith("not found barcode: 12345")

    mock_client.folio_get.assert_called_once_with(
        "/inventory/items", key="items", query="barcode==12345"
    )


def test_check_update_item_multiple_items_found(mocker):
    """Test when multiple items are found with the same barcode"""
    mock_client = MagicMock()
    mock_client.folio_get.return_value = [{"id": "123"}, {"id": "456"}]

    result = check_update_item("12345", mock_client, STAT_CODE_LOOKUP)

    assert result["error"].startswith("multiple items found for barcode: 12345")


def test_check_update_item_folio_get_exception(mocker):
    """Test when folio_get raises an exception"""
    mock_client = MagicMock()
    mock_client.folio_get.side_effect = Exception("Connection error")

    result = check_update_item("12345", mock_client, STAT_CODE_LOOKUP)

    assert result["error"].startswith("Connection error for barcode: 12345")


def test_check_update_item_no_update_needed(mocker):
    """Test when item has correct stat codes and no update is needed"""
    mock_client = MagicMock()
    item = {
        "id": "item-123",
        "statisticalCodeIds": [
            "af75fbdf-37f1-4d4c-a6ca-0b6ce0539342",
            "8d496de6-da6f-4a7b-9349-2409e2d8d4c2",
        ],
    }
    mock_client.folio_get.return_value = [item]

    result = check_update_item("12345", mock_client, STAT_CODE_LOOKUP)

    assert result == {}
    mock_client.folio_put.assert_not_called()


def test_check_update_item_removes_stat_codes(mocker):
    """Test when item has stat codes that need to be removed"""
    mock_client = MagicMock()
    item = {
        "id": "item-123",
        "statisticalCodeIds": [
            "38c2a160-9a46-4198-a5cb-8517fb7c2cee",
            "4d0ff7ad-cd0f-4d1a-8683-d63162e41b04",
        ],
    }
    mock_client.folio_get.return_value = [item]

    result = check_update_item("12345", mock_client, STAT_CODE_LOOKUP)

    assert result == {}
    mock_client.folio_put.assert_called_once()
    updated_item = mock_client.folio_put.call_args[1]["payload"]
    assert updated_item["statisticalCodeIds"] == [
        "af75fbdf-37f1-4d4c-a6ca-0b6ce0539342"
    ]


def test_check_update_item_adds_and_removes_stat_codes(mocker):
    """Test when item needs stat codes both added and removed"""
    mock_client = MagicMock()
    item = {
        "id": "item-123",
        "statisticalCodeIds": [
            "4d0ff7ad-cd0f-4d1a-8683-d63162e41b04",
            "8d496de6-da6f-4a7b-9349-2409e2d8d4c2",
        ],
    }
    mock_client.folio_get.return_value = [item]

    result = check_update_item("12345", mock_client, STAT_CODE_LOOKUP)

    assert result == {}
    mock_client.folio_put.assert_called_once()
    updated_item = mock_client.folio_put.call_args[1]["payload"]
    assert set(updated_item["statisticalCodeIds"]) == {
        "af75fbdf-37f1-4d4c-a6ca-0b6ce0539342",
        "8d496de6-da6f-4a7b-9349-2409e2d8d4c2",
    }


def test_check_update_item_folio_put_exception(mocker):
    """Test when folio_put raises an exception"""
    mock_client = MagicMock()
    item = {
        "id": "item-123",
        "statisticalCodeIds": [],
    }
    mock_client.folio_get.return_value = [item]
    mock_client.folio_put.side_effect = Exception("Update failed")

    result = check_update_item("12345", mock_client, STAT_CODE_LOOKUP)

    assert result == {"error": "Update failed for barcode: 12345"}


def test_delete_barcode_csv_existing_file(tmp_path):
    """Test deleting a file that exists"""

    tmp_file = tmp_path / "digi-scan-file.csv"

    tmp_file.touch()

    # Verify file exists
    assert tmp_file.exists()

    # Delete the file
    delete_barcode_csv(str(tmp_file))

    # Verify file is deleted
    assert not tmp_file.exists()


def test_delete_barcode_csv_nonexistent_file(tmp_path):
    """Test deleting a file that doesn't exist (should not raise error)"""
    tmp_file = tmp_path / "nonexistent_file_12345.csv"

    # Should not raise an error
    delete_barcode_csv(str(tmp_file))

    # Verify file still doesn't exist
    assert not tmp_file.exists()


def test_extract_barcodes_single_batch(mocker, tmp_path):
    """Test extracting barcodes that fit in a single batch"""
    # Mock Variable.get to return a batch size
    mocker.patch(
        'libsys_airflow.plugins.sdr.helpers.Variable.get', return_value="10000"
    )

    barcode_csv = tmp_path / "digi-send-barcodes.csv"

    DIGI_DF.to_csv(barcode_csv, index=False)

    batches = extract_barcodes(str(barcode_csv))

    assert len(batches) == 1
    assert set(batches[0]) == {"111", "222", "333"}


def test_extract_barcodes_multiple_batches(mocker, tmp_path):
    """Test extracting barcodes that require multiple batches"""
    # Mock Variable.get to return a small batch size
    mocker.patch('libsys_airflow.plugins.sdr.helpers.Variable.get', return_value="2")

    barcode_csv = tmp_path / "digi-sample.csv"

    DIGI_DF.to_csv(barcode_csv, index=False)

    batches = extract_barcodes(str(barcode_csv))

    assert len(batches) == 2
    assert set(batches[0]) == {"111", "222"}
    assert set(batches[1]) == {"333"}


def test_extract_barcodes_removes_duplicates(mocker, tmp_path):
    """Test that duplicate barcodes are removed from batches"""
    mocker.patch(
        'libsys_airflow.plugins.sdr.helpers.Variable.get', return_value="10000"
    )

    barcode_csv = tmp_path / "digi-dup-barcodes.csv"

    # Create a CSV with duplicate barcodes
    pd.DataFrame({"barcode": ["111", "222", "111", "333", "222", "444"]}).to_csv(
        barcode_csv, index=False
    )

    batches = extract_barcodes(str(barcode_csv))

    assert len(batches) == 1
    # Duplicates should be removed
    assert set(batches[0]) == {"111", "222", "333", "444"}
    assert len(batches[0]) == 4


def test_extract_barcodes_file_not_found(mocker):
    """Test error when CSV file doesn't exist"""
    with pytest.raises(ValueError, match="doesn't exist"):
        extract_barcodes("/nonexistent_file_12345.csv")


def test_extract_barcodes_missing_barcode_column(mocker, tmp_path):
    """Test error when CSV doesn't have a 'barcode' column"""
    mocker.patch(
        'libsys_airflow.plugins.sdr.helpers.Variable.get', return_value="10000"
    )

    missing_column_csv = tmp_path / "missing-barcodes.csv"

    pd.DataFrame({"item_id": ["111", "222", "333"]}).to_csv(
        missing_column_csv, index=False
    )

    with pytest.raises(ValueError, match="Column barcode required"):
        extract_barcodes(str(missing_column_csv))


def test_stat_code_lookup(mocker):
    mock_client = MagicMock()
    mock_client.statistical_codes = [
        {"code": "DIGI-SDR", "id": "af75fbdf-37f1-4d4c-a6ca-0b6ce0539342"},
        {"code": "DIGI-SCAN", "id": "38c2a160-9a46-4198-a5cb-8517fb7c2cee"},
        {"code": "DIGI-SENT", "id": "4d0ff7ad-cd0f-4d1a-8683-d63162e41b04"},
    ]

    stat_codes = stat_codes_lookup(mock_client)

    assert stat_codes["ADD"] == ["af75fbdf-37f1-4d4c-a6ca-0b6ce0539342"]
    assert stat_codes["REMOVE"] == sorted(
        ["38c2a160-9a46-4198-a5cb-8517fb7c2cee", "4d0ff7ad-cd0f-4d1a-8683-d63162e41b04"]
    )
