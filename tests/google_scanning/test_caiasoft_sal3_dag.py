import pytest

from datetime import datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from airflow.exceptions import AirflowException


@pytest.fixture(scope="module")
def dag():
    from libsys_airflow.dags.google_scanning.caiasoft_sal3 import (
        google_scanning_caiasoft_sal3,
    )

    return google_scanning_caiasoft_sal3()


def test_fetch_manifest_calls_courier_manifest_for_yesterday(dag, mocker):
    mock_wrapper = mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.CaiaSoftAPIWrapper"
    )
    mock_wrapper.return_value.courier_manifest.return_value = {
        "manifest": [],
        "success": True,
    }
    data_interval_start = datetime(
        2026, 8, 13, 6, 0, tzinfo=ZoneInfo("America/Los_Angeles")
    )

    result = dag.task_dict["fetch_manifest"].python_callable(
        data_interval_start=data_interval_start
    )

    args, kwargs = mock_wrapper.return_value.courier_manifest.call_args
    ship_from, ship_to = args
    assert ship_from == ship_to == "20260813"
    assert kwargs == {"courier": "GOOGLE"}
    assert result == {
        "manifest": {"manifest": [], "success": True},
        "date": "20260813",
        "dispatched": [],
    }


def test_fetch_manifest_uses_logical_date_not_wall_clock(dag, mocker):
    mock_wrapper = mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.CaiaSoftAPIWrapper"
    )
    mock_wrapper.return_value.courier_manifest.return_value = {
        "manifest": [],
        "success": True,
    }
    # 2026-08-14 03:00 UTC is 2026-08-13 20:00 Pacific (PDT, UTC-7), so a
    # retry after midnight UTC must still resolve to the same Pacific date.
    data_interval_start = datetime(2026, 8, 14, 3, 0, tzinfo=ZoneInfo("UTC"))

    result = dag.task_dict["fetch_manifest"].python_callable(
        data_interval_start=data_interval_start
    )

    assert result["date"] == "20260813"


def test_check_shipments_returns_no_shipments_when_none_dispatched(dag):
    result = dag.task_dict["check_shipments"].python_callable(
        {"dispatched": [], "date": "20260813"}
    )

    assert result == "no_shipments"


def test_check_shipments_returns_gather_barcodes_when_dispatched(dag):
    result = dag.task_dict["check_shipments"].python_callable(
        {"dispatched": [{"shipment": "SHIP-1"}], "date": "20260813"}
    )

    assert result == "gather_barcodes"


def test_no_shipments_logs_and_returns_none(dag, caplog):
    result = dag.task_dict["no_shipments"].python_callable(
        {"manifest": {}, "date": "20260813"}
    )

    assert result is None
    assert "No dispatched CaiaSoft shipments for 20260813" in caplog.text


def test_gather_barcodes_returns_pairs_and_shipment_numbers(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.barcode_bin_pairs",
        return_value=[("111", "BIN-1")],
    )

    result = dag.task_dict["gather_barcodes"].python_callable(
        {"dispatched": [{"shipment": "SHIP-1"}], "date": "20260813"}
    )

    assert result == {
        "pairs": [("111", "BIN-1")],
        "shipment_numbers": ["SHIP-1"],
    }


def test_gather_barcodes_raises_when_no_pairs(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.barcode_bin_pairs",
        return_value=[],
    )

    with pytest.raises(AirflowException, match="No barcodes found"):
        dag.task_dict["gather_barcodes"].python_callable(
            {"dispatched": [{"shipment": "SHIP-1"}], "date": "20260813"}
        )


def test_resolve_instances_returns_instance_ids_and_failures(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.folio_client",
        return_value=mocker.MagicMock(),
    )
    mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.resolve_instance_ids",
        return_value=({"111": "instance-1"}, []),
    )

    result = dag.task_dict["resolve_instances"].python_callable(
        {"pairs": [("111", "BIN-1")]}
    )

    assert result == {
        "instance_ids": {"111": "instance-1"},
        "instance_id_failures": [],
    }


def test_resolve_instances_raises_when_none_resolved(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.folio_client",
        return_value=mocker.MagicMock(),
    )
    mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.resolve_instance_ids",
        return_value=({}, [{"barcode": "111", "cart_name": "BIN-1", "reason": "boom"}]),
    )

    with pytest.raises(AirflowException, match="No barcodes resolved"):
        dag.task_dict["resolve_instances"].python_callable(
            {"pairs": [("111", "BIN-1")]}
        )


def test_generate_marc_calls_generate_marc_for_instances(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.sal3_filestamp",
        return_value="stanford_20260813-sal3",
    )
    mock_generate = mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.generate_marc_for_instances",
        return_value={
            "filestamp": "stanford_20260813-sal3",
            "marc_xml_path": "x.xml",
            "not_found_instance_ids": [],
        },
    )

    result = dag.task_dict["generate_marc"].python_callable(
        {"instance_ids": {"111": "instance-1", "222": "instance-1"}},
        {"date": "20260813"},
    )

    mock_generate.assert_called_once_with(
        ["instance-1", "instance-1"], "stanford_20260813-sal3"
    )
    assert result == {
        "filestamp": "stanford_20260813-sal3",
        "marc_xml_path": "x.xml",
        "not_found_instance_ids": [],
    }


def test_generate_sal3_manifest_calls_generate_manifest(dag, mocker):
    mock_generate_manifest = mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.generate_manifest",
        return_value="manifest.txt",
    )

    result = dag.task_dict["generate_sal3_manifest"].python_callable(
        {"pairs": [("111", "BIN-1"), ("222", "BIN-2")]},
        {"instance_ids": {"111": "instance-1"}},
        {"filestamp": "stamp"},
    )

    mock_generate_manifest.assert_called_once_with([("111", "BIN-1")], "stamp")
    assert result == "manifest.txt"


def test_combine_upload_files(dag):
    result = dag.task_dict["combine_upload_files"].python_callable(
        {"marc_xml_path": "x.xml"}, "manifest.txt"
    )

    assert result == ["x.xml", "manifest.txt"]


def test_check_upload_success(dag):
    result = dag.task_dict["check_upload"].python_callable({"failures": []})
    assert result == ["archive_transmitted_data_task", "build_sal3_result"]


def test_check_upload_failure(dag):
    result = dag.task_dict["check_upload"].python_callable({"failures": ["x.xml"]})
    assert result == "mark_failed_status"


def test_build_sal3_result(dag):
    result = dag.task_dict["build_sal3_result"].python_callable(
        {"pairs": [("111", "BIN-1"), ("222", "BIN-2")], "shipment_numbers": ["SHIP-1"]},
        {"instance_id_failures": []},
        {"not_found_instance_ids": [], "marc_xml_path": "x.xml"},
        "manifest.txt",
        {"date": "20260813"},
    )

    assert result == {
        "date": "20260813",
        "shipment_numbers": ["SHIP-1"],
        "shipped_barcode_count": 2,
        "instance_id_failures": [],
        "not_found_instance_ids": [],
        "marc_xml_path": "x.xml",
        "manifest_path": "manifest.txt",
    }


def test_mark_failed_status(dag):
    result = dag.task_dict["mark_failed_status"].python_callable(
        {"failures": ["x.xml"]}
    )

    assert result == "Failed to upload files to Google Drive: ['x.xml']"


def test_raise_shipment_failure(dag):
    with pytest.raises(AirflowException, match="boom"):
        dag.task_dict["raise_shipment_failure"].python_callable("boom")


def test_check_upload_branch_wiring(dag):
    downstream_ids = {t.task_id for t in dag.get_task("check_upload").downstream_list}
    assert downstream_ids == {
        "archive_transmitted_data_task",
        "build_sal3_result",
        "mark_failed_status",
    }


def test_failure_callback_attached_only_upstream_of_branch(dag):
    from libsys_airflow.dags.google_scanning.caiasoft_sal3 import _notify_failure

    for task_id in [
        "fetch_manifest",
        "check_shipments",
        "gather_barcodes",
        "resolve_instances",
        "generate_marc",
        "generate_sal3_manifest",
        "combine_upload_files",
        "upload_to_drive_task",
    ]:
        assert _notify_failure in dag.get_task(task_id).on_failure_callback

    assert not dag.get_task("build_sal3_result").on_failure_callback
    assert not dag.get_task("mark_failed_status").on_failure_callback


def test_notify_failure_sends_email(mocker):
    from libsys_airflow.dags.google_scanning.caiasoft_sal3 import _notify_failure

    mock_send_email = mocker.patch(
        "libsys_airflow.dags.google_scanning.caiasoft_sal3.send_sal3_failure_email"
    )
    mock_dag_run = MagicMock()
    mock_ti = MagicMock()
    mock_ti.task_id = "gather_barcodes"

    context = {
        "task_instance": mock_ti,
        "dag_run": mock_dag_run,
        "exception": ValueError("boom"),
    }

    _notify_failure(context)

    mock_send_email.assert_called_once()
    reason, dag_run = mock_send_email.call_args[0]
    assert "gather_barcodes failed" in reason
    assert "boom" in reason
    assert dag_run is mock_dag_run
