import pytest

from unittest.mock import MagicMock

from airflow.exceptions import AirflowException

from libsys_airflow.plugins.google_scanning.constants import (
    STAGED_FILES_BASE,
    STATUS_SHIPPED,
)


@pytest.fixture(scope="module")
def dag():
    from libsys_airflow.dags.google_scanning.on_campus_shipment import (
        on_campus_shipment,
    )

    return on_campus_shipment()


def test_setup_returns_selected_carts_and_shipped_at(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.get_current_context",
        return_value={
            "params": {
                "selected_carts": [{"cart_name": "cart-1", "filename": "b.txt"}],
                "shipped_at": "20260810",
            }
        },
    )

    result = dag.task_dict["setup"].python_callable()

    assert result == {
        "selected_carts": [{"cart_name": "cart-1", "filename": "b.txt"}],
        "shipped_at": "20260810",
    }


def test_setup_raises_when_selected_carts_missing(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.get_current_context",
        return_value={"params": {"shipped_at": "20260810"}},
    )

    with pytest.raises(ValueError, match="Missing selected_carts"):
        dag.task_dict["setup"].python_callable()


def test_setup_raises_when_shipped_at_missing(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.get_current_context",
        return_value={"params": {"selected_carts": []}},
    )

    with pytest.raises(ValueError, match="Missing shipped_at"):
        dag.task_dict["setup"].python_callable()


def test_gather_barcodes_returns_to_ship_and_skipped(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.barcodes_for_shipment",
        return_value=([("111", "cart-1")], []),
    )

    result = dag.task_dict["gather_barcodes"].python_callable(
        {"selected_carts": [{"cart_name": "cart-1", "filename": "b.txt"}]}
    )

    assert result == {"to_ship": [("111", "cart-1")], "skipped": []}


def test_gather_barcodes_raises_when_nothing_to_ship(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.barcodes_for_shipment",
        return_value=([], [{"cart_name": "cart-1", "barcodes": ["111"]}]),
    )

    with pytest.raises(AirflowException, match="No barcodes available to ship"):
        dag.task_dict["gather_barcodes"].python_callable({"selected_carts": []})


def test_resolve_instances_returns_instance_ids_and_failures(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.folio_client",
        return_value=mocker.MagicMock(),
    )
    mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.resolve_instance_ids",
        return_value=({"111": "instance-1"}, []),
    )

    result = dag.task_dict["resolve_instances"].python_callable(
        {"to_ship": [("111", "cart-1")]}
    )

    assert result == {
        "instance_ids": {"111": "instance-1"},
        "instance_id_failures": [],
    }


def test_resolve_instances_raises_when_none_resolved(dag, mocker):
    mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.folio_client",
        return_value=mocker.MagicMock(),
    )
    mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.resolve_instance_ids",
        return_value=(
            {},
            [{"barcode": "111", "cart_name": "cart-1", "reason": "boom"}],
        ),
    )

    with pytest.raises(AirflowException, match="No barcodes resolved"):
        dag.task_dict["resolve_instances"].python_callable(
            {"to_ship": [("111", "cart-1")]}
        )


def test_generate_marc_calls_generate_shipment_marc(dag, mocker):
    mock_generate = mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.generate_shipment_marc",
        return_value={
            "filestamp": "stamp",
            "marc_xml_path": "x.xml",
            "not_found_instance_ids": [],
        },
    )

    result = dag.task_dict["generate_marc"].python_callable(
        {"instance_ids": {"111": "instance-1", "222": "instance-1"}},
        {"shipped_at": "20260810"},
    )

    mock_generate.assert_called_once_with(["instance-1", "instance-1"], "20260810")
    assert result == {
        "filestamp": "stamp",
        "marc_xml_path": "x.xml",
        "not_found_instance_ids": [],
    }


def test_generate_shipment_manifest_calls_generate_manifest(dag, mocker):
    mock_generate_manifest = mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.generate_manifest",
        return_value="manifest.txt",
    )

    result = dag.task_dict["generate_shipment_manifest"].python_callable(
        {"to_ship": [("111", "cart-1")]}, {"filestamp": "stamp"}
    )

    mock_generate_manifest.assert_called_once_with([("111", "cart-1")], "stamp")
    assert result == "manifest.txt"


def test_combine_upload_files(dag):
    result = dag.task_dict["combine_upload_files"].python_callable(
        {"marc_xml_path": "x.xml"}, "manifest.txt"
    )

    assert result == ["x.xml", "manifest.txt"]


def test_check_upload_success(dag):
    result = dag.task_dict["check_upload"].python_callable({"failures": []})
    assert result == "archive_and_mark_shipped"


def test_check_upload_failure(dag):
    result = dag.task_dict["check_upload"].python_callable({"failures": ["x.xml"]})
    assert result == "mark_failed_status"


def test_archive_and_mark_shipped(dag, mocker):
    mock_update_status = mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.update_cart_status"
    )
    mock_archive = mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.archive_shipped_cart"
    )
    mock_dag_run = MagicMock()
    mock_dag_run.run_id = "run-123"

    result = dag.task_dict["archive_and_mark_shipped"].python_callable(
        {"to_ship": [("111", "cart-1"), ("222", "cart-2")]},
        {
            "selected_carts": [{"cart_name": "cart-1"}, {"cart_name": "cart-2"}],
            "shipped_at": "20260810",
        },
        dag_run=mock_dag_run,
    )

    assert result == ["cart-1", "cart-2"]
    assert mock_update_status.call_count == 2
    mock_update_status.assert_any_call(
        "cart-1",
        STAGED_FILES_BASE,
        status=STATUS_SHIPPED,
        shipped_at="20260810",
        shipment_dag_run_id="run-123",
    )
    assert mock_archive.call_count == 2
    mock_archive.assert_any_call("cart-1")
    mock_archive.assert_any_call("cart-2")


def test_build_shipment_result(dag):
    result = dag.task_dict["build_shipment_result"].python_callable(
        ["cart-1", "cart-2"],
        {"to_ship": [("111", "cart-1"), ("222", "cart-2")], "skipped": []},
        {"instance_id_failures": []},
        {"not_found_instance_ids": [], "marc_xml_path": "x.xml"},
        "manifest.txt",
    )

    assert result == {
        "shipped_carts": ["cart-1", "cart-2"],
        "shipped_barcode_count": 2,
        "skipped": [],
        "instance_id_failures": [],
        "not_found_instance_ids": [],
        "marc_xml_path": "x.xml",
        "manifest_path": "manifest.txt",
    }


def test_mark_failed_status(dag, mocker):
    mock_mark_carts_failed = mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.mark_carts_failed"
    )
    mock_dag_run = MagicMock()
    mock_dag_run.run_id = "run-123"

    result = dag.task_dict["mark_failed_status"].python_callable(
        {"failures": ["x.xml"]},
        {"selected_carts": [{"cart_name": "cart-1"}]},
        dag_run=mock_dag_run,
    )

    mock_mark_carts_failed.assert_called_once_with([{"cart_name": "cart-1"}], "run-123")
    assert result == "Failed to upload files to Google Drive: ['x.xml']"


def test_raise_shipment_failure(dag):
    with pytest.raises(AirflowException, match="boom"):
        dag.task_dict["raise_shipment_failure"].python_callable("boom")


def test_check_upload_branch_wiring(dag):
    downstream_ids = {t.task_id for t in dag.get_task("check_upload").downstream_list}
    assert downstream_ids == {
        "archive_and_mark_shipped",
        "archive_transmitted_data_task",
        "mark_failed_status",
    }


def test_failure_callback_attached_only_upstream_of_branch(dag):
    from libsys_airflow.dags.google_scanning.on_campus_shipment import (
        _mark_failed_and_notify,
    )

    assert (
        _mark_failed_and_notify in dag.get_task("gather_barcodes").on_failure_callback
    )
    assert (
        _mark_failed_and_notify in dag.get_task("resolve_instances").on_failure_callback
    )
    assert not dag.get_task("archive_and_mark_shipped").on_failure_callback
    assert not dag.get_task("mark_failed_status").on_failure_callback


def test_mark_failed_and_notify_marks_carts_and_sends_email(mocker):
    from libsys_airflow.dags.google_scanning.on_campus_shipment import (
        _mark_failed_and_notify,
    )

    mock_mark_carts_failed = mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.mark_carts_failed"
    )
    mock_send_email = mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.send_shipment_failure_email"
    )
    mock_dag_run = MagicMock()
    mock_dag_run.run_id = "run-123"
    mock_ti = MagicMock()
    mock_ti.task_id = "gather_barcodes"

    context = {
        "task_instance": mock_ti,
        "dag_run": mock_dag_run,
        "params": {
            "selected_carts": [{"cart_name": "cart-1"}, {"cart_name": "cart-2"}],
            "user_email": "staff@example.com",
        },
        "exception": ValueError("boom"),
    }

    _mark_failed_and_notify(context)

    mock_mark_carts_failed.assert_called_once_with(
        [{"cart_name": "cart-1"}, {"cart_name": "cart-2"}], "run-123"
    )
    mock_send_email.assert_called_once()
    reason, dag_run, user_email = mock_send_email.call_args[0]
    assert "gather_barcodes failed" in reason
    assert "boom" in reason
    assert dag_run is mock_dag_run
    assert user_email == "staff@example.com"


def test_mark_failed_and_notify_defaults_selected_carts_and_user_email(mocker):
    from libsys_airflow.dags.google_scanning.on_campus_shipment import (
        _mark_failed_and_notify,
    )

    mock_mark_carts_failed = mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.mark_carts_failed"
    )
    mock_send_email = mocker.patch(
        "libsys_airflow.dags.google_scanning.on_campus_shipment.send_shipment_failure_email"
    )
    mock_ti = MagicMock()
    mock_ti.task_id = "resolve_instances"

    context = {
        "task_instance": mock_ti,
        "dag_run": None,
        "params": {},
        "exception": ValueError("boom"),
    }

    _mark_failed_and_notify(context)

    mock_mark_carts_failed.assert_called_once_with([], None)
    reason, dag_run, user_email = mock_send_email.call_args[0]
    assert dag_run is None
    assert user_email is None
