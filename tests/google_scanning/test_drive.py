import pytest  # noqa

from libsys_airflow.plugins.google_scanning.drive import (
    upload_to_drive_task,
    drive_folder_id,
    DEFAULT_DRIVE_FOLDER_ID,
)


@pytest.fixture
def mock_files(tmp_path):
    files = []
    for name in ["stanford_20260101-campus.xml", "stanford_20260101-campus.txt"]:
        file_path = tmp_path / name
        file_path.write_text("hello world")
        files.append(str(file_path))
    return files


def test_drive_folder_id_default(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.drive.Variable.get",
        side_effect=lambda key, default: default,
    )
    assert drive_folder_id() == DEFAULT_DRIVE_FOLDER_ID


def test_drive_folder_id_from_variable(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.drive.Variable.get",
        return_value="abc123",
    )
    assert drive_folder_id() == "abc123"


def test_upload_to_drive_task_skips_when_not_production(mocker, mock_files, caplog):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.drive.is_production",
        return_value=False,
    )

    result = upload_to_drive_task.function(mock_files)

    assert result == {"success": mock_files, "failures": []}
    assert "SKIPPING GOOGLE DRIVE UPLOAD" in caplog.text


def test_upload_to_drive_task_success(mocker, mock_files, caplog):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.drive.is_production",
        return_value=True,
    )
    mock_upload_file = mocker.patch(
        "airflow.providers.google.suite.hooks.drive.GoogleDriveHook.upload_file",
        return_value="file-id",
    )
    mocker.patch(
        "airflow.providers.google.suite.hooks.drive.GoogleDriveHook.__init__",
        return_value=None,
    )

    result = upload_to_drive_task.function(mock_files)

    assert result["success"] == mock_files
    assert result["failures"] == []
    assert mock_upload_file.call_count == 2
    assert "Uploaded" in caplog.text


def test_upload_to_drive_task_failure(mocker, mock_files, caplog):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.drive.is_production",
        return_value=True,
    )
    mocker.patch(
        "airflow.providers.google.suite.hooks.drive.GoogleDriveHook.__init__",
        return_value=None,
    )
    mocker.patch(
        "airflow.providers.google.suite.hooks.drive.GoogleDriveHook.upload_file",
        side_effect=Exception("boom"),
    )

    result = upload_to_drive_task.function(mock_files)

    assert result["success"] == []
    assert result["failures"] == mock_files
    assert "Error uploading" in caplog.text


def test_upload_to_drive_task_partial_failure_rolls_back_uploaded_file(
    mocker, mock_files, caplog
):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.drive.is_production",
        return_value=True,
    )
    mocker.patch(
        "airflow.providers.google.suite.hooks.drive.GoogleDriveHook.__init__",
        return_value=None,
    )
    mocker.patch(
        "airflow.providers.google.suite.hooks.drive.GoogleDriveHook.upload_file",
        side_effect=["file-id-1", Exception("boom")],
    )
    mock_get_conn = mocker.patch(
        "airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn"
    )
    mock_delete = mock_get_conn.return_value.files.return_value.delete

    result = upload_to_drive_task.function(mock_files)

    assert result["success"] == []
    assert result["failures"] == [mock_files[1]]
    mock_delete.assert_called_once_with(fileId="file-id-1", supportsAllDrives=True)
    mock_delete.return_value.execute.assert_called_once()
    assert "Rolled back partial upload" in caplog.text


def test_upload_to_drive_task_rollback_failure_is_logged(mocker, mock_files, caplog):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.drive.is_production",
        return_value=True,
    )
    mocker.patch(
        "airflow.providers.google.suite.hooks.drive.GoogleDriveHook.__init__",
        return_value=None,
    )
    mocker.patch(
        "airflow.providers.google.suite.hooks.drive.GoogleDriveHook.upload_file",
        side_effect=["file-id-1", Exception("boom")],
    )
    mock_get_conn = mocker.patch(
        "airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn"
    )
    mock_get_conn.return_value.files.return_value.delete.return_value.execute.side_effect = Exception(
        "delete boom"
    )

    result = upload_to_drive_task.function(mock_files)

    assert result["success"] == []
    assert result["failures"] == [mock_files[1]]
    assert "Failed to roll back" in caplog.text
