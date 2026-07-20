from airflow.providers.fab.www import app as application
from bs4 import BeautifulSoup
from flask.wrappers import Response
from io import BytesIO
from unittest.mock import patch, MagicMock
import pytest

from conftest import root_directory
from libsys_airflow.plugins.boundwith.boundwith_view import BoundWithView


@pytest.fixture
def test_airflow_client():
    templates_folder = f"{root_directory}/libsys_airflow/plugins/boundwith/templates"

    app = application.create_app(enable_plugins=False)
    app.config['WTF_CSRF_ENABLED'] = False

    with app.app_context():
        app.appbuilder.add_view(BoundWithView, "BoundWith", category="Boundwith")
        app.blueprints['BoundWithView'].template_folder = templates_folder

    app.response_class = HTMLResponse

    with app.test_client() as client:
        yield client


class HTMLResponse(Response):
    @property
    def html(self):
        return BeautifulSoup(self.get_data(), "html.parser")


def test_bw_home(test_airflow_client):
    """Test boundwith home view loads successfully."""
    response = test_airflow_client.get('/boundwith/')
    assert response.status_code == 200


def test_run_bw_creation_missing_file(test_airflow_client):
    """Test upload fails when file is not provided."""
    response = test_airflow_client.post('/boundwith/create', data={'sunid': 'testuser'})
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "Missing Boundwith Relationship File" in alert


def test_run_bw_creation_missing_sunid(test_airflow_client):
    """Test upload fails when SUNID is not provided."""
    csv_data = b"part_holdings_hrid,principle_barcode\nHR001,BC001\n"
    file = (BytesIO(csv_data), "test.csv")

    response = test_airflow_client.post(
        '/boundwith/create', data={'upload-boundwith': file}
    )
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "SUNID Required" in alert


def test_run_bw_creation_empty_sunid(test_airflow_client):
    """Test upload fails when SUNID is empty string."""
    csv_data = b"part_holdings_hrid,principle_barcode\nHR001,BC001\n"
    file = (BytesIO(csv_data), "test.csv")

    response = test_airflow_client.post(
        '/boundwith/create', data={'upload-boundwith': file, 'sunid': '   '}
    )
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "SUNID Required" in alert


def test_run_bw_creation_invalid_columns(test_airflow_client):
    """Test upload fails when CSV has invalid columns."""
    csv_data = b"wrong_col1,wrong_col2\nHR001,BC001\n"
    file = (BytesIO(csv_data), "test.csv")

    response = test_airflow_client.post(
        '/boundwith/create', data={'upload-boundwith': file, 'sunid': 'testuser'}
    )
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "Invalid columns" in alert


def test_run_bw_creation_too_many_rows(test_airflow_client):
    """Test upload warns when CSV has more than 1000 rows."""
    # Create a CSV with 1001 rows
    rows = ["part_holdings_hrid,principle_barcode"]
    for i in range(1001):
        rows.append(f"HR{i},BC{i}")
    csv_data = "\n".join(rows).encode()
    file = (BytesIO(csv_data), "test.csv")

    response = test_airflow_client.post(
        '/boundwith/create', data={'upload-boundwith': file, 'sunid': 'testuser'}
    )
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "Warning! CSV file has 1001 rows, limit is 1,000" in alert


def test_run_bw_creation_empty_csv(test_airflow_client):
    """Test upload warns when CSV file is empty."""
    csv_data = b""
    file = (BytesIO(csv_data), "test.csv")

    response = test_airflow_client.post(
        '/boundwith/create', data={'upload-boundwith': file, 'sunid': 'testuser'}
    )
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "Empty CSV file" in alert


@patch('libsys_airflow.plugins.boundwith.boundwith_view.DagRunApi')
@patch('libsys_airflow.plugins.boundwith.boundwith_view.api_client')
def test_run_bw_creation_success_with_correct_conf(
    mock_api_client, mock_dag_run_api, test_airflow_client
):
    """Test successful boundwith creation with correct DAG configuration."""
    csv_data = b"part_holdings_hrid,principle_barcode\nHR001,BC001\nHR002,BC002\n"
    file = (BytesIO(csv_data), "boundwith_test.csv")

    # Setup mocks
    mock_api_instance = MagicMock()
    mock_dag_run_api.return_value = mock_api_instance

    mock_api_response = MagicMock()
    mock_api_response.dag_run_id = "bw_dag_run_456"
    mock_api_instance.trigger_dag_run.return_value = mock_api_response

    response = test_airflow_client.post(
        '/boundwith/create',
        data={
            'upload-boundwith': file,
            'sunid': 'jdoe',
            'user-email': 'jdoe@example.com',
        },
    )
    assert response.status_code == 200

    # Verify DagRunApi was instantiated
    mock_dag_run_api.assert_called_once()

    # Verify trigger_dag_run was called with correct parameters
    assert mock_api_instance.trigger_dag_run.call_count == 1
    call_args = mock_api_instance.trigger_dag_run.call_args

    dag_id = call_args[0][0]
    trigger_body = call_args[0][1]

    assert dag_id == "add_bw_relationships"

    # Verify the configuration parameters
    assert trigger_body.conf['sunid'] == 'jdoe'
    assert trigger_body.conf['email'] == 'jdoe@example.com'
    assert trigger_body.conf['file_name'] == 'boundwith_test.csv'

    # Verify relationships data structure
    relationships = trigger_body.conf['relationships']
    assert isinstance(relationships, list)
    assert len(relationships) == 2
    assert relationships[0] == {
        'part_holdings_hrid': 'HR001',
        'principle_barcode': 'BC001',
    }
    assert relationships[1] == {
        'part_holdings_hrid': 'HR002',
        'principle_barcode': 'BC002',
    }


@patch('libsys_airflow.plugins.boundwith.boundwith_view.DagRunApi')
@patch('libsys_airflow.plugins.boundwith.boundwith_view.api_client')
def test_run_bw_creation_success_with_no_email(
    mock_api_client, mock_dag_run_api, test_airflow_client
):
    """Test successful boundwith creation when no email is provided."""
    csv_data = b"part_holdings_hrid,principle_barcode\nHR001,BC001\nHR002,BC002\n"
    file = (BytesIO(csv_data), "boundwith_test.csv")

    # Setup mocks
    mock_api_instance = MagicMock()
    mock_dag_run_api.return_value = mock_api_instance

    mock_api_response = MagicMock()
    mock_api_response.dag_run_id = "bw_dag_run_789"
    mock_api_instance.trigger_dag_run.return_value = mock_api_response

    response = test_airflow_client.post(
        '/boundwith/create', data={'upload-boundwith': file, 'sunid': 'jdoe'}
    )
    assert response.status_code == 200

    # Verify trigger_dag_run was called
    call_args = mock_api_instance.trigger_dag_run.call_args
    trigger_body = call_args[0][1]

    # Verify email is None when not provided
    assert trigger_body.conf['email'] is None
    assert trigger_body.conf['sunid'] == 'jdoe'


@patch('libsys_airflow.plugins.boundwith.boundwith_view.pd.read_csv')
def test_run_bw_creation_csv_error(mock_read_csv, test_airflow_client):
    """Test upload handles general CSV parsing errors."""
    csv_data = b"part_holdings_hrid,principle_barcode\nHR001,BC001\n"
    file = (BytesIO(csv_data), "test.csv")

    # Mock pd.read_csv to raise a ParserError
    mock_read_csv.side_effect = Exception("CSV parsing failed: invalid format")

    response = test_airflow_client.post(
        '/boundwith/create', data={'upload-boundwith': file, 'sunid': 'testuser'}
    )
    assert response.status_code == 200

    alert = response.html.find(class_="alert-message").get_text()
    assert "Error with CSV" in alert and "CSV parsing failed" in alert
