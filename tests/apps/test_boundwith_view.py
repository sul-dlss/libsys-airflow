from io import BytesIO
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient
import pytest  # noqa

from libsys_airflow.plugins.boundwith.boundwith_view import app

client = TestClient(app)


def test_bw_home():
    """Test boundwith home view loads successfully."""
    response = client.get('/')
    assert response.status_code == 200


def test_run_bw_creation_missing_file():
    """Test upload fails when file is not provided."""
    response = client.post('/create', data={'sunid': 'testuser'})
    assert response.status_code == 200
    assert "Missing Boundwith Relationship File" in response.text


def test_run_bw_creation_missing_sunid():
    """Test upload fails when SUNID is not provided."""
    csv_data = b"part_holdings_hrid,principle_barcode\nHR001,BC001\n"

    response = client.post(
        '/create',
        files={'upload_boundwith': ("test.csv", BytesIO(csv_data), "text/csv")},
    )
    assert response.status_code == 200
    assert "SUNID Required" in response.text


def test_run_bw_creation_empty_sunid():
    """Test upload fails when SUNID is empty string."""
    csv_data = b"part_holdings_hrid,principle_barcode\nHR001,BC001\n"

    response = client.post(
        '/create',
        data={'sunid': '   '},
        files={'upload_boundwith': ("test.csv", BytesIO(csv_data), "text/csv")},
    )
    assert response.status_code == 200
    assert "SUNID Required" in response.text


def test_run_bw_creation_invalid_columns():
    """Test upload fails when CSV has invalid columns."""
    csv_data = b"wrong_col1,wrong_col2\nHR001,BC001\n"

    response = client.post(
        '/create',
        data={'sunid': 'testuser'},
        files={'upload_boundwith': ("test.csv", BytesIO(csv_data), "text/csv")},
    )
    assert response.status_code == 200
    assert "Invalid columns" in response.text


def test_run_bw_creation_too_many_rows():
    """Test upload warns when CSV has more than 1000 rows."""
    rows = ["part_holdings_hrid,principle_barcode"]
    for i in range(1001):
        rows.append(f"HR{i},BC{i}")
    csv_data = "\n".join(rows).encode()

    response = client.post(
        '/create',
        data={'sunid': 'testuser'},
        files={'upload_boundwith': ("test.csv", BytesIO(csv_data), "text/csv")},
    )
    assert response.status_code == 200
    assert "Warning! CSV file has 1001 rows, limit is 1,000" in response.text


def test_run_bw_creation_empty_csv():
    """Test upload warns when CSV file is empty."""
    csv_data = b""

    response = client.post(
        '/create',
        data={'sunid': 'testuser'},
        files={'upload_boundwith': ("test.csv", BytesIO(csv_data), "text/csv")},
    )
    assert response.status_code == 200
    assert "Empty CSV file" in response.text


@patch('libsys_airflow.plugins.boundwith.boundwith_view.DagRunApi')
@patch('libsys_airflow.plugins.boundwith.boundwith_view.api_client')
def test_run_bw_creation_success_with_correct_conf(mock_api_client, mock_dag_run_api):
    """Test successful boundwith creation with correct DAG configuration."""
    csv_data = b"part_holdings_hrid,principle_barcode\nHR001,BC001\nHR002,BC002\n"

    mock_api_instance = MagicMock()
    mock_dag_run_api.return_value = mock_api_instance

    mock_api_response = MagicMock()
    mock_api_response.dag_run_id = "bw_dag_run_456"
    mock_api_instance.trigger_dag_run.return_value = mock_api_response

    response = client.post(
        '/create',
        data={'sunid': 'jdoe', 'user_email': 'jdoe@example.com'},
        files={
            'upload_boundwith': ("boundwith_test.csv", BytesIO(csv_data), "text/csv")
        },
    )
    assert response.status_code == 200

    mock_dag_run_api.assert_called_once()

    assert mock_api_instance.trigger_dag_run.call_count == 1
    call_args = mock_api_instance.trigger_dag_run.call_args

    dag_id = call_args[0][0]
    trigger_body = call_args[0][1]

    assert dag_id == "add_bw_relationships"

    assert trigger_body.conf['sunid'] == 'jdoe'
    assert trigger_body.conf['email'] == 'jdoe@example.com'
    assert trigger_body.conf['file_name'] == 'boundwith_test.csv'

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
def test_run_bw_creation_success_with_no_email(mock_api_client, mock_dag_run_api):
    """Test successful boundwith creation when no email is provided."""
    csv_data = b"part_holdings_hrid,principle_barcode\nHR001,BC001\nHR002,BC002\n"

    mock_api_instance = MagicMock()
    mock_dag_run_api.return_value = mock_api_instance

    mock_api_response = MagicMock()
    mock_api_response.dag_run_id = "bw_dag_run_789"
    mock_api_instance.trigger_dag_run.return_value = mock_api_response

    response = client.post(
        '/create',
        data={'sunid': 'jdoe'},
        files={
            'upload_boundwith': ("boundwith_test.csv", BytesIO(csv_data), "text/csv")
        },
    )
    assert response.status_code == 200

    call_args = mock_api_instance.trigger_dag_run.call_args
    trigger_body = call_args[0][1]

    assert trigger_body.conf['email'] is None
    assert trigger_body.conf['sunid'] == 'jdoe'


@patch('libsys_airflow.plugins.boundwith.boundwith_view.pd.read_csv')
def test_run_bw_creation_csv_error(mock_read_csv):
    """Test upload handles general CSV parsing errors."""
    csv_data = b"part_holdings_hrid,principle_barcode\nHR001,BC001\n"

    mock_read_csv.side_effect = Exception("CSV parsing failed: invalid format")

    response = client.post(
        '/create',
        data={'sunid': 'testuser'},
        files={'upload_boundwith': ("test.csv", BytesIO(csv_data), "text/csv")},
    )
    assert response.status_code == 200
    assert "Error with CSV" in response.text and "CSV parsing failed" in response.text
