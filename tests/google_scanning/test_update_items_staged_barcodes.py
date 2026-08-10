import pytest

from unittest.mock import MagicMock

from airflow.exceptions import AirflowException


@pytest.fixture(scope="module")
def dag():
    from libsys_airflow.dags.google_scanning.update_items_staged_barcodes import (
        stage_cart_items,
    )

    return stage_cart_items()


def test_setup_returns_cart_name_and_staged_file_path(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.get_current_context",
        return_value={
            "params": {
                "cart_name": "Cart 1",
                "staged_file_path": "/opt/airflow/data-export-files/google_scanning/staged/cart-1.txt",
            }
        },
    )

    result = dag.task_dict["setup"].python_callable()

    assert result == {
        "cart_name": "Cart 1",
        "staged_file_path": "/opt/airflow/data-export-files/google_scanning/staged/cart-1.txt",
    }


def test_setup_raises_when_staged_file_path_missing(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.get_current_context",
        return_value={"params": {"cart_name": "Cart 1"}},
    )

    with pytest.raises(ValueError, match="Missing staged_file_path"):
        dag.task_dict["setup"].python_callable()


def test_setup_raises_when_cart_name_missing(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.get_current_context",
        return_value={"params": {"staged_file_path": "/opt/airflow/staged/cart-1.txt"}},
    )

    with pytest.raises(ValueError, match="Missing cart name"):
        dag.task_dict["setup"].python_callable()


def test_get_and_batch_barcodes_single_batch(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.read_staged_barcode_files",
        return_value=["111", "222", "333"],
    )
    mock_ti = MagicMock()
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.get_current_context",
        return_value={"ti": mock_ti},
    )

    result = dag.task_dict["get_and_batch_barcodes"].python_callable(
        staged_file_path="/opt/airflow/staged/cart-1.txt"
    )

    assert result == [["111", "222", "333"]]
    mock_ti.xcom_push.assert_called_once_with(key="total", value=3)


def test_get_and_batch_barcodes_multiple_batches(dag, mocker):
    barcodes = [str(i) for i in range(25)]
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.read_staged_barcode_files",
        return_value=barcodes,
    )
    mock_ti = MagicMock()
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.get_current_context",
        return_value={"ti": mock_ti},
    )

    result = dag.task_dict["get_and_batch_barcodes"].python_callable(
        staged_file_path="/opt/airflow/staged/cart-1.txt"
    )

    assert len(result) > 1
    assert sum(len(batch) for batch in result) == 25
    mock_ti.xcom_push.assert_called_once_with(key="total", value=25)


def test_folio_uuids_returns_ids_from_helper(dag, mocker):
    mock_client = MagicMock()
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.folio_client",
        return_value=mock_client,
    )
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.Variable.get",
        return_value="GRE-GOOGLE-SCANNING-ONCAMPUS",
    )
    mock_get_folio_uuids = mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.get_folio_uuids",
        return_value=("temp-location-uuid", "digi-sent-uuid", "note-type-uuid"),
    )

    result = dag.task_dict["folio_uuids"].python_callable()

    mock_get_folio_uuids.assert_called_once_with(
        mock_client, "GRE-GOOGLE-SCANNING-ONCAMPUS"
    )
    assert result == {
        "digi_sent_id": "digi-sent-uuid",
        "note_type_id": "note-type-uuid",
        "temp_location_id": "temp-location-uuid",
    }


def test_process_barcodes_batch_counts_success_missing_and_errors(dag, mocker):
    def side_effect(**kwargs):
        match kwargs["barcode"]:
            case "found":
                return {}
            case "missing":
                return {"missing": "missing"}
            case _:
                return {"barcode": kwargs["barcode"], "reason": "boom"}

    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.folio_client",
        return_value=MagicMock(),
    )
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.process_barcode",
        side_effect=side_effect,
    )

    result = dag.task_dict["process_barcodes_batch"].python_callable(
        batch=["found", "missing", "error"],
        temp_location_id="temp-location-uuid",
        digi_sent_id="digi-sent-uuid",
        note_type_id="note-type-uuid",
    )

    assert result == {
        "errors": [{"barcode": "error", "reason": "boom"}],
        "missing": ["missing"],
        "successful_updates": 1,
    }


def test_process_barcodes_batch_passes_kwargs_to_process_barcode(dag, mocker):
    mock_process_barcode = mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.process_barcode",
        return_value={},
    )
    mock_client = MagicMock()
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.folio_client",
        return_value=mock_client,
    )

    dag.task_dict["process_barcodes_batch"].python_callable(
        batch=["111"],
        temp_location_id="temp-location-uuid",
        digi_sent_id="digi-sent-uuid",
        note_type_id="note-type-uuid",
    )

    mock_process_barcode.assert_called_once_with(
        barcode="111",
        folio_client=mock_client,
        temp_location_id="temp-location-uuid",
        digi_sent_id="digi-sent-uuid",
        note_type_id="note-type-uuid",
    )


def test_process_barcodes_batch_all_missing(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.folio_client",
        return_value=MagicMock(),
    )
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.process_barcode",
        return_value={"missing": "111"},
    )

    result = dag.task_dict["process_barcodes_batch"].python_callable(
        batch=["111"],
        temp_location_id="temp-location-uuid",
        digi_sent_id="digi-sent-uuid",
        note_type_id="note-type-uuid",
    )

    assert result == {"errors": [], "missing": ["111"], "successful_updates": 0}


def test_generate_status_json_aggregates_batch_results(dag, mocker):
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = 5
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.get_current_context",
        return_value={"ti": mock_ti},
    )
    mock_write_status_json = mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.write_status_json",
        return_value=True,
    )
    init_params = {"staged_file_path": "/opt/airflow/staged/cart-1.txt"}
    batch_results = [
        {"successful_updates": 2, "missing": ["111"], "errors": []},
        {
            "successful_updates": 1,
            "missing": [],
            "errors": [{"barcode": "222", "reason": "boom"}],
        },
    ]

    dag.task_dict["generate_status_json"].python_callable(init_params, batch_results)

    mock_ti.xcom_pull.assert_called_once_with(
        task_ids="get_and_batch_barcodes", key="total"
    )
    mock_write_status_json.assert_called_once_with(
        init_params,
        5,
        {
            "successful_updates": 3,
            "missing": ["111"],
            "errors": [{"barcode": "222", "reason": "boom"}],
        },
    )


def test_generate_status_json_raises_on_failure(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.get_current_context",
        return_value={"ti": MagicMock()},
    )
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.write_status_json",
        return_value=False,
    )

    with pytest.raises(AirflowException, match="Failed to write status.json"):
        dag.task_dict["generate_status_json"].python_callable(
            {"staged_file_path": "/opt/airflow/staged/cart-1.txt"},
            [{"successful_updates": 0, "missing": [], "errors": []}],
        )


def test_generate_status_json_returns_none_on_success(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.get_current_context",
        return_value={"ti": MagicMock()},
    )
    mocker.patch(
        "libsys_airflow.dags.google_scanning.update_items_staged_barcodes.write_status_json",
        return_value=True,
    )

    result = dag.task_dict["generate_status_json"].python_callable(
        {"staged_file_path": "/opt/airflow/staged/cart-1.txt"},
        [{"successful_updates": 1, "missing": [], "errors": []}],
    )

    assert result is None
