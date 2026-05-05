from airflow.providers.fab.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response
from io import BytesIO
from unittest.mock import patch, MagicMock
import pytest

from conftest import root_directory
from libsys_airflow.plugins.data_exports.apps.data_export_upload_view import (
    DataExportUploadView,
)


@pytest.fixture
def test_airflow_client():
    templates_folder = f"{root_directory}/libsys_airflow/plugins/data_exports/templates"

    app = application.create_app(enable_plugins=False)
    app.config['WTF_CSRF_ENABLED'] = False

    with app.app_context():
        app.appbuilder.add_view(
            DataExportUploadView, "DataExport", category="Data export"
        )
        app.blueprints['DataExportUploadView'].template_folder = templates_folder

    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")


def test_upload_view(test_airflow_client):
    response = test_airflow_client.get('/data_export_upload/')
    assert response.status_code == 200

    upload_vendors = response.html.find(id="vendor").find_all('option')
    assert upload_vendors[-1].get_text() == "sharevde"


def test_create_upload_missing_file(test_airflow_client):
    response = test_airflow_client.post('/data_export_upload/create')
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "Missing Instance UUID File" in alert


def test_run_data_export_upload_missing_vendor(test_airflow_client):
    """Test upload fails when vendor is not selected."""
    csv_data = b"12345678-1234-1234-1234-123456789012\n"
    file = (BytesIO(csv_data), "test.csv")

    response = test_airflow_client.post(
        '/data_export_upload/create',
        data={
            'upload-data-export-ids': file,
            'kind': 'new',
            'user_email': 'test@example.com'
        }
    )
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "You must choose a vendor!" in alert


def test_run_data_export_upload_missing_kind(test_airflow_client):
    """Test upload fails when kind is not selected."""
    csv_data = b"12345678-1234-1234-1234-123456789012\n"
    file = (BytesIO(csv_data), "test.csv")

    response = test_airflow_client.post(
        '/data_export_upload/create',
        data={
            'upload-data-export-ids': file,
            'vendor': 'oclc',
            'user_email': 'test@example.com'
        }
    )
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "You must select an option for New records, Updates or Deletes!" in alert


def test_run_data_export_upload_invalid_uuid(test_airflow_client):
    """Test upload fails when UUID format is invalid."""
    csv_data = b"not-a-valid-uuid\n"
    file = (BytesIO(csv_data), "test.csv")

    response = test_airflow_client.post(
        '/data_export_upload/create',
        data={
            'upload-data-export-ids': file,
            'vendor': 'oclc',
            'kind': 'new',
            'user_email': 'test@example.com'
        }
    )
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "is not a UUID" in alert


def test_run_data_export_upload_empty_file(test_airflow_client):
    """Test upload warns when CSV file is empty."""
    csv_data = b""
    file = (BytesIO(csv_data), "test.csv")

    response = test_airflow_client.post(
        '/data_export_upload/create',
        data={
            'upload-data-export-ids': file,
            'vendor': 'oclc',
            'kind': 'new',
            'user_email': 'test@example.com'
        }
    )
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "Empty UUID file" in alert


@patch('libsys_airflow.plugins.data_exports.apps.data_export_upload_view.upload_data_export_ids')
@patch.object(DataExportUploadView, '_trigger_dag_run')
def test_run_data_export_upload_success(
    mock_trigger_dag_run, mock_upload_data_export_ids, test_airflow_client
):
    """Test successful upload with valid UUID and DAG trigger."""
    csv_data = b"12345678-1234-1234-1234-123456789012\n87654321-4321-4321-4321-210987654321\n"
    file = (BytesIO(csv_data), "test.csv")

    # Mock the upload and trigger functions
    mock_upload_data_export_ids.return_value = ["/path/to/ids.txt", 2]
    mock_trigger_dag_run.return_value = "dag_run_123"

    response = test_airflow_client.post(
        '/data_export_upload/create',
        data={
            'upload-data-export-ids': file,
            'vendor': 'oclc',
            'kind': 'new',
            'user_email': 'test@example.com'
        }
    )
    assert response.status_code == 200

    # Verify success messages appear
    alerts = response.html.find_all(class_="alert-message")
    alert_texts = [alert.get_text() for alert in alerts]
    assert any("Sucessfully uploaded ID file with 2 IDs" in text for text in alert_texts)
    assert any("Starting oclc DAG run dag_run_123" in text for text in alert_texts)

    # Verify mocked functions were called correctly
    mock_upload_data_export_ids.assert_called_once()
    mock_trigger_dag_run.assert_called_once()


@patch('libsys_airflow.plugins.data_exports.apps.data_export_upload_view.upload_data_export_ids')
def test_run_data_export_upload_multiple_columns(
    mock_upload_data_export_ids, test_airflow_client
):
    """Test upload fails when CSV has more than one column."""
    csv_data = b"id1,id2\n12345678-1234-1234-1234-123456789012,87654321-4321-4321-4321-210987654321\n"
    file = (BytesIO(csv_data), "test.csv")

    mock_upload_data_export_ids.side_effect = ValueError("ID file has more than one column.")

    response = test_airflow_client.post(
        '/data_export_upload/create',
        data={
            'upload-data-export-ids': file,
            'vendor': 'oclc',
            'kind': 'new',
            'user_email': 'test@example.com'
        }
    )
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "ID file has more than one column" in alert


@patch('libsys_airflow.plugins.data_exports.apps.data_export_upload_view.upload_data_export_ids')
@patch('libsys_airflow.plugins.data_exports.apps.data_export_upload_view.DagRunApi')
@patch('libsys_airflow.plugins.data_exports.apps.data_export_upload_view.api_client')
def test_run_data_export_upload_with_dag_config(
    mock_api_client, mock_dag_run_api, mock_upload_data_export_ids, test_airflow_client
):
    """Test that DAG is triggered with correct configuration parameters."""
    csv_data = b"12345678-1234-1234-1234-123456789012\n"
    file = (BytesIO(csv_data), "test_ids.csv")

    # Setup mocks
    mock_upload_data_export_ids.return_value = ["/path/to/ids.txt", 1]
    
    mock_api_instance = MagicMock()
    mock_dag_run_api.return_value = mock_api_instance
    
    mock_api_response = MagicMock()
    mock_api_response.dag_run_id = "test_dag_run_123"
    mock_api_instance.trigger_dag_run.return_value = mock_api_response

    response = test_airflow_client.post(
        '/data_export_upload/create',
        data={
            'upload-data-export-ids': file,
            'vendor': 'oclc',
            'kind': 'new',
            'user_email': 'user@example.com'
        }
    )
    assert response.status_code == 200

    # Verify DagRunApi was instantiated with the api_client context manager result
    mock_dag_run_api.assert_called_once()
    
    # Verify trigger_dag_run was called with correct dag_id
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
