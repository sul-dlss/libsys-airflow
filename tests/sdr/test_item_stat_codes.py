import logging

import pandas as pd
import pytest

from unittest.mock import MagicMock

STAT_CODE_LOOKUP = {
    "ADD": ["af75fbdf-37f1-4d4c-a6ca-0b6ce0539342"],
    "REMOVE": [
        "38c2a160-9a46-4198-a5cb-8517fb7c2cee",
        "4d0ff7ad-cd0f-4d1a-8683-d63162e41b04",
    ],
}


@pytest.fixture(scope="module")
def dag():
    from libsys_airflow.dags.sdr.item_stat_codes import manage_sdr_stat_codes

    return manage_sdr_stat_codes()


def test_setup_dag_routes_to_csv_branch(dag, mocker):
    """setup_dag routes to get_batches_from_csv and xcom-pushes the file path"""
    mock_ti = MagicMock()
    mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes.get_current_context",
        return_value={
            "params": {"kwargs": {"file": "/opt/airflow/sdr-files/barcodes.csv"}}
        },
    )

    result = dag.task_dict["setup_stat_code_management"].python_callable(ti=mock_ti)

    assert result == "get_batches_from_csv"
    mock_ti.xcom_push.assert_called_once_with(
        key="csv_file", value="/opt/airflow/sdr-files/barcodes.csv"
    )


def test_setup_dag_routes_to_sdr_pull(dag, mocker):
    """setup_dag routes to sdr_pull_items when no csv file is provided"""
    mock_ti = MagicMock()
    mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes.get_current_context",
        return_value={"params": {"kwargs": {"file": None}}},
    )

    result = dag.task_dict["setup_stat_code_management"].python_callable(ti=mock_ti)

    assert result == "sdr_pull_items"
    mock_ti.xcom_push.assert_not_called()


def test_get_batches_from_csv(dag, mocker, tmp_path):
    """get_batches_from_csv xcom-pulls the csv path and returns batched barcodes"""
    csv_file = tmp_path / "barcodes.csv"
    pd.DataFrame({"barcode": ["111", "222", "333"]}).to_csv(csv_file, index=False)

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = str(csv_file)
    mocker.patch(
        "libsys_airflow.plugins.sdr.helpers.Variable.get", return_value="10000"
    )

    result = dag.task_dict["get_batches_from_csv"].python_callable(ti=mock_ti)

    mock_ti.xcom_pull.assert_called_once_with(
        task_ids="setup_stat_code_management", key="csv_file"
    )
    assert len(result) == 1
    assert set(result[0]) == {"111", "222", "333"}


def test_get_stat_code_lookup(dag, mocker):
    """get_stat_code_lookup returns ADD and REMOVE uuid lists from folio client"""
    mock_client = MagicMock()
    mock_client.statistical_codes = [
        {"code": "DIGI-SDR", "id": "af75fbdf-37f1-4d4c-a6ca-0b6ce0539342"},
        {"code": "DIGI-SCAN", "id": "38c2a160-9a46-4198-a5cb-8517fb7c2cee"},
        {"code": "DIGI-SENT", "id": "4d0ff7ad-cd0f-4d1a-8683-d63162e41b04"},
    ]
    mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes._folio_client",
        return_value=mock_client,
    )

    result = dag.task_dict["stat_code_lookup"].python_callable()

    assert result["ADD"] == ["af75fbdf-37f1-4d4c-a6ca-0b6ce0539342"]
    assert set(result["REMOVE"]) == {
        "38c2a160-9a46-4198-a5cb-8517fb7c2cee",
        "4d0ff7ad-cd0f-4d1a-8683-d63162e41b04",
    }


def test_process_barcode_batch_success(dag, mocker):
    """process_barcode_batch does not xcom_push when all barcodes are found"""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = STAT_CODE_LOOKUP
    mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes._folio_client",
        return_value=MagicMock(),
    )
    mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes.check_update_item", return_value={}
    )

    dag.task_dict["process_barcode_batch"].python_callable(
        batch=["111", "222", "333"], ti=mock_ti
    )

    mock_ti.xcom_push.assert_not_called()


def test_process_barcode_batch_missing_barcode(dag, mocker):
    """process_barcode_batch saves missing barcodes to a file and xcom-pushes its path"""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = STAT_CODE_LOOKUP
    mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes._folio_client",
        return_value=MagicMock(),
    )

    def check_side_effect(barcode, client, lookup):
        if barcode == "missing":
            return {"error": "not found barcode: missing"}
        return {}

    mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes.check_update_item",
        side_effect=check_side_effect,
    )
    mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes.save_missing_barcodes",
        return_value="/tmp/abc123.csv",
    )

    dag.task_dict["process_barcode_batch"].python_callable(
        batch=["111", "missing"], ti=mock_ti
    )

    mock_ti.xcom_push.assert_called_once_with(
        key="missing_barcode_file", value="/tmp/abc123.csv"
    )


def test_process_barcode_batch_api_error(dag, mocker, caplog):
    """process_barcode_batch logs non-missing errors without saving a missing-barcodes file"""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = STAT_CODE_LOOKUP
    mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes._folio_client",
        return_value=MagicMock(),
    )
    mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes.check_update_item",
        return_value={"error": "Connection error for barcode: 111"},
    )

    dag.task_dict["process_barcode_batch"].python_callable(
        batch=["111"], ti=mock_ti
    )

    mock_ti.xcom_push.assert_not_called()
    assert "111 error: Connection error" in caplog.text


def test_combine_missing_barcode_files(dag, mocker):
    """combine_missing_barcode_files xcom-pulls with map_indexes=None and calls concat"""
    mock_ti = MagicMock()
    mock_files = ["/tmp/file1.csv", "/tmp/file2.csv"]
    mock_ti.xcom_pull.return_value = mock_files
    mock_concat = mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes.concat_missing_barcodes"
    )

    dag.task_dict["combine_missing_barcode_files"].python_callable(ti=mock_ti)

    mock_ti.xcom_pull.assert_called_once_with(
        task_ids="process_barcode_batch",
        key="missing_barcode_file",
        map_indexes=None,
    )
    mock_concat.assert_called_once_with(mock_files)


def test_remove_csv_file(dag, mocker):
    """remove_csv_file xcom-pulls the csv path and calls delete_barcode_csv"""
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "/opt/airflow/sdr-files/barcodes.csv"
    mock_delete = mocker.patch(
        "libsys_airflow.dags.sdr.item_stat_codes.delete_barcode_csv"
    )

    dag.task_dict["remove_csv_file"].python_callable(ti=mock_ti)

    mock_ti.xcom_pull.assert_called_once_with(
        task_ids="setup_stat_code_management", key="csv_file"
    )
    mock_delete.assert_called_once_with("/opt/airflow/sdr-files/barcodes.csv")


def test_sdr_pull_items(dag, caplog):
    """sdr_pull_items logs that the SDR API pull is not yet available"""
    with caplog.at_level(logging.INFO):
        dag.task_dict["sdr_pull_items"].python_callable()

    assert "SDR API pull not available" in caplog.text
