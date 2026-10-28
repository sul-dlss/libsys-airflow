import pytest

from datetime import date


@pytest.fixture(scope="module")
def dag():
    from libsys_airflow.dags.google_scanning.send_google_scanning_files import (
        send_google_scanning_files,
    )

    return send_google_scanning_files()


def _downstream(dag, task_id: str) -> set[str]:
    return {task.task_id for task in dag.get_task(task_id).downstream_list}


def test_task_wiring(dag):
    assert _downstream(dag, "gather_files_task") == {"select_sendable_files"}
    assert _downstream(dag, "select_sendable_files") == {
        "has_files_to_send",
        "upload_to_drive_task",
        "transmission_failure_email",
    }
    assert _downstream(dag, "has_files_to_send") == {"upload_to_drive_task"}
    assert _downstream(dag, "upload_to_drive_task") == {
        "archive_shipments",
        "move_transmitted_data_task",
        "transmission_failure_email",
    }
    assert _downstream(dag, "archive_shipments") == {"move_transmitted_data_task"}


def test_failure_email_runs_even_when_upload_is_skipped(dag):
    assert dag.get_task("transmission_failure_email").trigger_rule == "all_done"
    assert dag.get_task("has_files_to_send").ignore_downstream_trigger_rules is False


def test_select_sendable_files(dag, mocker):
    mock_sendable = mocker.patch(
        "libsys_airflow.dags.google_scanning.send_google_scanning_files.sendable_shipment_files",
        return_value={"file_list": ["a.xml", "a.txt"], "skipped": []},
    )
    mocker.patch(
        "libsys_airflow.dags.google_scanning.send_google_scanning_files.pacific_today",
        return_value=date(2026, 8, 10),
    )

    result = dag.task_dict["select_sendable_files"].python_callable(
        {"file_list": ["a.xml", "a.txt"], "s3": False}
    )

    mock_sendable.assert_called_once_with(["a.xml", "a.txt"], "20260810")
    assert result == {"file_list": ["a.xml", "a.txt"], "skipped": []}


def test_has_files_to_send(dag):
    has_files = dag.task_dict["has_files_to_send"].python_callable
    assert has_files({"file_list": ["a.xml"]}) is True
    assert has_files({"file_list": []}) is False


def test_archive_shipments(dag, mocker):
    mock_archive = mocker.patch(
        "libsys_airflow.dags.google_scanning.send_google_scanning_files.archive_sent_shipments",
        return_value=["archived/cart-1/ts"],
    )

    result = dag.task_dict["archive_shipments"].python_callable(
        {"success": ["a.xml", "a.txt"], "failures": []}
    )

    mock_archive.assert_called_once_with(["a.xml", "a.txt"])
    assert result == ["archived/cart-1/ts"]
