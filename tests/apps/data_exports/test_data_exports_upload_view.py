from io import BytesIO
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient
from csrf_helpers import csrf_test_client  # noqa
import pytest  # noqa

from libsys_airflow.plugins.data_exports.apps.data_export_upload_view import app

client = csrf_test_client(app)


def test_upload_view():
    response = client.get('/')
    assert response.status_code == 200

    assert 'value="sharevde"' in response.text


def test_create_upload_missing_file():
    response = client.post('/create')
    assert response.status_code == 200

    assert "Missing Instance UUID File" in response.text


def test_run_data_export_upload_missing_vendor():
    """Test upload fails when vendor is not selected."""
    csv_data = b"12345678-1234-1234-1234-123456789012\n"

    response = client.post(
        '/create',
        data={'kind': 'new', 'user_email': 'test@example.com'},
        files={'ids_file': ('test.csv', BytesIO(csv_data), 'text/csv')},
    )
    assert response.status_code == 200

    assert "You must choose a vendor!" in response.text


def test_run_data_export_upload_missing_kind():
    """Test upload fails when kind is not selected."""
    csv_data = b"12345678-1234-1234-1234-123456789012\n"

    response = client.post(
        '/create',
        data={'vendor': 'oclc', 'user_email': 'test@example.com'},
        files={'ids_file': ('test.csv', BytesIO(csv_data), 'text/csv')},
    )
    assert response.status_code == 200

    assert (
        "You must select an option for New records, Updates or Deletes!"
        in response.text
    )


def test_run_data_export_upload_invalid_uuid():
    """Test upload fails when UUID format is invalid."""
    csv_data = b"not-a-valid-uuid\n"

    response = client.post(
        '/create',
        data={'vendor': 'oclc', 'kind': 'new', 'user_email': 'test@example.com'},
        files={'ids_file': ('test.csv', BytesIO(csv_data), 'text/csv')},
    )
    assert response.status_code == 200

    assert "is not a UUID" in response.text


def test_run_data_export_upload_empty_file():
    """Test upload warns when CSV file is empty."""
    csv_data = b""

    response = client.post(
        '/create',
        data={'vendor': 'oclc', 'kind': 'new', 'user_email': 'test@example.com'},
        files={'ids_file': ('test.csv', BytesIO(csv_data), 'text/csv')},
    )
    assert response.status_code == 200

    assert "Empty UUID file" in response.text


@patch(
    'libsys_airflow.plugins.data_exports.apps.data_export_upload_view.upload_data_export_ids'
)
@patch(
    'libsys_airflow.plugins.data_exports.apps.data_export_upload_view._trigger_dag_run'
)
def test_run_data_export_upload_success(
    mock_trigger_dag_run, mock_upload_data_export_ids
):
    """Test successful upload with valid UUID and DAG trigger."""
    csv_data = (
        b"12345678-1234-1234-1234-123456789012\n87654321-4321-4321-4321-210987654321\n"
    )

    mock_upload_data_export_ids.return_value = ["/path/to/ids.txt", 2]
    mock_trigger_dag_run.return_value = "dag_run_123"

    response = client.post(
        '/create',
        data={'vendor': 'oclc', 'kind': 'new', 'user_email': 'test@example.com'},
        files={'ids_file': ('test.csv', BytesIO(csv_data), 'text/csv')},
    )
    assert response.status_code == 200

    assert "Sucessfully uploaded ID file with 2 IDs" in response.text
    assert "Starting oclc DAG run dag_run_123" in response.text

    mock_upload_data_export_ids.assert_called_once()
    mock_trigger_dag_run.assert_called_once()


@patch(
    'libsys_airflow.plugins.data_exports.apps.data_export_upload_view.upload_data_export_ids'
)
def test_run_data_export_upload_multiple_columns(mock_upload_data_export_ids):
    """Test upload fails when CSV has more than one column."""
    csv_data = b"id1,id2\n12345678-1234-1234-1234-123456789012,87654321-4321-4321-4321-210987654321\n"

    mock_upload_data_export_ids.side_effect = ValueError(
        "ID file has more than one column."
    )

    response = client.post(
        '/create',
        data={'vendor': 'oclc', 'kind': 'new', 'user_email': 'test@example.com'},
        files={'ids_file': ('test.csv', BytesIO(csv_data), 'text/csv')},
    )
    assert response.status_code == 200

    assert "ID file has more than one column" in response.text


@patch(
    'libsys_airflow.plugins.data_exports.apps.data_export_upload_view.upload_data_export_ids'
)
@patch('libsys_airflow.plugins.data_exports.apps.data_export_upload_view.DagRunApi')
@patch('libsys_airflow.plugins.data_exports.apps.data_export_upload_view.api_client')
def test_run_data_export_upload_with_dag_config(
    mock_api_client, mock_dag_run_api, mock_upload_data_export_ids
):
    """Test that DAG is triggered with correct configuration parameters."""
    csv_data = b"12345678-1234-1234-1234-123456789012\n"

    mock_upload_data_export_ids.return_value = ["/path/to/ids.txt", 1]

    mock_api_instance = MagicMock()
    mock_dag_run_api.return_value = mock_api_instance

    mock_api_response = MagicMock()
    mock_api_response.dag_run_id = "test_dag_run_123"
    mock_api_instance.trigger_dag_run.return_value = mock_api_response

    response = client.post(
        '/create',
        data={'vendor': 'oclc', 'kind': 'new', 'user_email': 'user@example.com'},
        files={'ids_file': ('test_ids.csv', BytesIO(csv_data), 'text/csv')},
    )
    assert response.status_code == 200

    mock_dag_run_api.assert_called_once()

    assert mock_api_instance.trigger_dag_run.call_count == 1
    call_args = mock_api_instance.trigger_dag_run.call_args

    dag_id = call_args[0][0]
    trigger_body = call_args[0][1]

    assert dag_id == "select_oclc_records"
    assert trigger_body.conf['fetch_folio_record_ids'] is False
    assert trigger_body.conf['saved_record_ids_kind'] == 'new'
    assert trigger_body.conf['user_email'] == 'user@example.com'
    assert trigger_body.conf['number_of_ids'] == 1
    assert trigger_body.conf['uploaded_filename'] == 'test_ids.csv'


def test_create_upload_without_csrf_token():
    response = TestClient(app).post('/create')

    assert response.status_code == 403
    assert response.json()["detail"] == "CSRF token missing or invalid"
