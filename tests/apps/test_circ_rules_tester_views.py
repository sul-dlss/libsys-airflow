import json

from io import BytesIO
from unittest.mock import MagicMock, patch
from urllib.parse import unquote_plus

from fastapi.testclient import TestClient
from csrf_helpers import csrf_test_client  # noqa
import pytest  # noqa

from libsys_airflow.plugins.folio.apps import circ_rules_tester_view
from libsys_airflow.plugins.folio.apps.circ_rules_tester_view import app

client = csrf_test_client(app, follow_redirects=False)


def test_circ_rules_tester_main_page():
    response = client.get("/")

    assert response.status_code == 200
    assert "<h2>FOLIO Circ Rules Tester</h2>" in response.text


def test_circ_rules_tester_reference_home(mocker):
    mocker.patch(
        'libsys_airflow.plugins.folio.apps.circ_rules_tester_view.folio_client'
    )

    response = client.get("/reference")

    assert response.status_code == 200
    assert "<h2>Reference Data</h2>" in response.text

    assert response.text.count('<li><a href="reference/') == 4


mock_patron_groups = [
    {
        'group': 'graduate',
        'desc': 'Graduate Student',
        'id': 'ad0bc554-d5bc-463c-85d1-5562127ae91b',
        'metadata': {
            'createdDate': '2023-08-09T20:12:40.204+00:00',
            'updatedDate': '2026-01-28T22:40:29.039+00:00',
        },
    },
    {
        'group': 'staff',
        'desc': 'Staff Member',
        'id': '3684a786-6671-4268-8ed0-9db82ebca60b',
        'expirationOffsetInDays': 730,
        'metadata': {
            'createdDate': '2023-08-09T20:12:40.000+00:00',
            'updatedDate': '2026-01-28T22:40:29.040+00:00',
        },
    },
    {
        'group': 'undergrad',
        'desc': 'Undergraduate Student',
        'id': 'bdc2b6d4-5ceb-4a12-ab46-249b9a68473e',
        'metadata': {
            'createdDate': '2023-08-09T20:12:39.103+00:00',
            'updatedDate': '2026-01-28T22:40:29.140+00:00',
        },
    },
]


def test_circ_rules_tester_patron_group(mocker):
    mock_folio_client = mocker.MagicMock()
    mock_folio_client.folio_get = lambda *args, **kwargs: mock_patron_groups

    mocker.patch(
        'libsys_airflow.plugins.folio.apps.circ_rules_tester_view.folio_client',
        return_value=mock_folio_client,
    )

    response = client.get("/reference/patron_group")

    assert response.status_code == 200
    assert "<h2>Patron Groups</h2>" in response.text

    assert response.text.count("<tr>") == 3

    assert "graduate" in response.text
    assert "Graduate Student" in response.text
    assert "ad0bc554" in response.text


def test_run_batch_test_missing_file():
    response = client.post("/batch_test")

    assert response.status_code == 200
    assert "No scenario file uploaded" in response.text


def test_run_batch_test_invalid_extension():
    response = client.post(
        "/batch_test",
        files={
            "upload_scenarios": ("scenarios.txt", BytesIO(b"not-a-csv"), "text/plain")
        },
    )

    assert response.status_code == 200
    assert "Scenario file must be a csv" in response.text


@patch('libsys_airflow.plugins.folio.apps.circ_rules_tester_view.DagRunApi')
@patch('libsys_airflow.plugins.folio.apps.circ_rules_tester_view.api_client')
def test_run_batch_test_success(mock_api_client, mock_dag_run_api):
    mock_api_instance = MagicMock()
    mock_dag_run_api.return_value = mock_api_instance
    mock_api_response = MagicMock()
    mock_api_response.dag_run_id = "batch-run-123"
    mock_api_instance.trigger_dag_run.return_value = mock_api_response

    csv_data = (
        b"patron_group_id,material_type_id,loan_type_id,location_id\n"
        b"pg1,mt1,lt1,loc1\n"
    )
    response = client.post(
        "/batch_test",
        files={"upload_scenarios": ("scenarios.csv", BytesIO(csv_data), "text/csv")},
    )

    assert response.status_code == 303
    assert response.headers["location"] == "batch_report/batch-run-123"

    call_args = mock_api_instance.trigger_dag_run.call_args
    assert call_args[0][0] == "circ_rules_batch_tests"


@patch('libsys_airflow.plugins.folio.apps.circ_rules_tester_view.DagRunApi')
@patch('libsys_airflow.plugins.folio.apps.circ_rules_tester_view.api_client')
def test_run_batch_test_dag_trigger_failure(mock_api_client, mock_dag_run_api):
    mock_api_instance = MagicMock()
    mock_dag_run_api.return_value = mock_api_instance
    mock_api_instance.trigger_dag_run.side_effect = Exception("dag not found")

    csv_data = (
        b"patron_group_id,material_type_id,loan_type_id,location_id\npg1,mt1,lt1,loc1\n"
    )
    response = client.post(
        "/batch_test",
        files={"upload_scenarios": ("scenarios.csv", BytesIO(csv_data), "text/csv")},
    )

    assert response.status_code == 200
    assert "Failed to trigger circ_rules_batch_tests DAG" in response.text


@patch('libsys_airflow.plugins.folio.apps.circ_rules_tester_view.DagRunApi')
@patch('libsys_airflow.plugins.folio.apps.circ_rules_tester_view.api_client')
def test_run_test_success(mock_api_client, mock_dag_run_api):
    mock_api_instance = MagicMock()
    mock_dag_run_api.return_value = mock_api_instance
    mock_api_response = MagicMock()
    mock_api_response.dag_run_id = "scenario-run-456"
    mock_api_instance.trigger_dag_run.return_value = mock_api_response

    response = client.post(
        "/test",
        data={
            "patron_group_id": "pg1",
            "material_type_id": "mt1",
            "loan_type_id": "lt1",
            "location_id": "loc1",
        },
    )

    assert response.status_code == 303
    assert response.headers["location"] == "report/scenario-run-456"

    call_args = mock_api_instance.trigger_dag_run.call_args
    dag_id = call_args[0][0]
    trigger_body = call_args[0][1]
    assert dag_id == "circ_rules_scenario_tests"
    assert trigger_body.conf == {
        "patron_group_id": "pg1",
        "material_type_id": "mt1",
        "loan_type_id": "lt1",
        "location_id": "loc1",
    }


@patch('libsys_airflow.plugins.folio.apps.circ_rules_tester_view.DagRunApi')
@patch('libsys_airflow.plugins.folio.apps.circ_rules_tester_view.api_client')
def test_run_test_dag_trigger_failure(mock_api_client, mock_dag_run_api):
    mock_api_instance = MagicMock()
    mock_dag_run_api.return_value = mock_api_instance
    mock_api_instance.trigger_dag_run.side_effect = Exception("dag not found")

    response = client.post("/test", data={"patron_group_id": "pg1"})

    assert response.status_code == 303
    assert response.headers["location"].startswith(".?")
    assert "Failed to Trigger circ_rules_scenario_test DAG" in unquote_plus(
        response.headers["location"]
    )


def test_report_batch_not_found(mocker, tmp_path):
    mocker.patch.object(circ_rules_tester_view, "CIRC_HOME", tmp_path)

    response = client.get("/batch_report/missing-run")

    assert response.status_code == 200
    assert "Report for DAG Run not completed. DAG ID missing-run" in response.text
    assert "Check for Report" in response.text


def test_report_batch_found(mocker, tmp_path):
    mocker.patch.object(circ_rules_tester_view, "CIRC_HOME", tmp_path)
    report_path = tmp_path / "batch-run-1.json"
    report_path.write_text(
        json.dumps([{"patron_group_id": "pg1", "result": "allowed"}]),
        encoding="utf-8-sig",
    )

    response = client.get("/batch_report/batch-run-1")

    assert response.status_code == 200
    assert "pg1" in response.text
    assert 'href="../download/batch-run-1"' in response.text


def test_download_report_missing(mocker, tmp_path):
    mocker.patch.object(circ_rules_tester_view, "CIRC_HOME", tmp_path)

    response = client.get("/download/missing-run")

    assert response.status_code == 303
    assert response.headers["location"].startswith("..?")
    assert "Batch report DAG ID missing-run doesn't exist" in unquote_plus(
        response.headers["location"]
    )


def test_download_report_found(mocker, tmp_path):
    mocker.patch.object(circ_rules_tester_view, "CIRC_HOME", tmp_path)
    report_path = tmp_path / "batch-run-2.json"
    report_path.write_text(
        json.dumps([{"patron_group_id": "pg1", "result": "allowed"}]),
        encoding="utf-8-sig",
    )

    response = client.get("/download/batch-run-2")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert (
        "attachment;filename=batch_report_" in response.headers["content-disposition"]
    )
    assert "pg1" in response.text


def test_report_scenario_not_found(mocker, tmp_path):
    mocker.patch.object(circ_rules_tester_view, "CIRC_HOME", tmp_path)

    response = client.get("/report/missing-run")

    assert response.status_code == 200
    assert "Report for DAG Run not completed. DAG ID missing-run" in response.text
    assert "Check for Report" in response.text


def test_report_scenario_found(mocker, tmp_path):
    mocker.patch.object(circ_rules_tester_view, "CIRC_HOME", tmp_path)
    report_path = tmp_path / "scenario-run-1.json"
    report_path.write_text(
        json.dumps({"Loan Policy": "Standard", "Result": "Allowed"}),
        encoding="utf-8-sig",
    )

    response = client.get("/report/scenario-run-1")

    assert response.status_code == 200
    assert "Loan Policy" in response.text
    assert "Allowed" in response.text


def test_batch_test_without_csrf_token():
    response = TestClient(app, follow_redirects=False).post("/batch_test")

    assert response.status_code == 403
    assert response.json()["detail"] == "CSRF token missing or invalid"


def test_single_test_without_csrf_token():
    response = TestClient(app, follow_redirects=False).post(
        "/test", data={"patron_group_id": "pg1"}
    )

    assert response.status_code == 403
