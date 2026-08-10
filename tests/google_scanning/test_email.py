import pytest  # noqa

from bs4 import BeautifulSoup

from libsys_airflow.plugins.google_scanning.email import (
    send_shipment_failure_email,
    shipment_confirmation_email,
    shipment_failure_email,
)


@pytest.fixture(autouse=True)
def mock_email_devs(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.email.Variable.get",
        return_value="devs@example.com",
    )


@pytest.fixture
def mock_dag_run(mocker):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual_2026-08-10"
    dag_run.dag_id = "on_campus_shipment"
    return dag_run


@pytest.fixture
def shipment_result():
    return {
        "shipped_carts": ["cart-1", "cart-2"],
        "shipped_barcode_count": 42,
        "skipped": [{"cart_name": "cart-1", "barcodes": ["999"]}],
        "instance_id_failures": [
            {
                "barcode": "888",
                "cart_name": "cart-2",
                "reason": "missing barcode: 888",
            }
        ],
        "not_found_instance_ids": ["8576f36e-0ab5-4146-9b6b-9f0b84f7fc74"],
        "marc_xml_path": "/opt/airflow/data-export-files/google_scanning/marc-files/new/stanford_20260810-campus-143022.xml",
        "manifest_path": "/opt/airflow/data-export-files/google_scanning/marc-files/manifests/stanford_20260810-campus-143022.txt",
    }


def test_shipment_confirmation_email(mocker, mock_dag_run, shipment_result):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.google_scanning.email.send_email_with_server_name"
    )

    shipment_confirmation_email.function(
        shipment_result,
        dag_run=mock_dag_run,
        params={"user_email": "staff@example.com"},
    )

    assert mock_send_email.called
    call_kwargs = mock_send_email.call_args[1]
    assert call_kwargs["to"] == ["devs@example.com", "staff@example.com"]
    assert call_kwargs["subject"] == "Google Scanning On-Campus Shipment Confirmation"

    html_body = BeautifulSoup(call_kwargs["html_content"], "html.parser")
    assert "42" in html_body.find("p").text
    list_items = [li.text for li in html_body.find_all("li")]
    assert "cart-1" in list_items
    assert "cart-2" in list_items
    assert shipment_result["marc_xml_path"] in list_items
    assert any("888" in item for item in list_items)
    assert any("8576f36e-0ab5-4146-9b6b-9f0b84f7fc74" in item for item in list_items)


def test_shipment_confirmation_email_no_user_email(
    mocker, mock_dag_run, shipment_result
):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.google_scanning.email.send_email_with_server_name"
    )

    shipment_confirmation_email.function(
        shipment_result, dag_run=mock_dag_run, params={}
    )

    assert mock_send_email.call_args[1]["to"] == ["devs@example.com"]


def test_shipment_confirmation_email_no_skips_or_failures(mocker, mock_dag_run):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.google_scanning.email.send_email_with_server_name"
    )

    shipment_confirmation_email.function(
        {
            "shipped_carts": ["cart-1"],
            "shipped_barcode_count": 1,
            "skipped": [],
            "instance_id_failures": [],
            "not_found_instance_ids": [],
            "marc_xml_path": "x.xml",
            "manifest_path": "x.txt",
        },
        dag_run=mock_dag_run,
        params={},
    )

    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )
    assert html_body.find("h3", string=lambda s: s and "skipped" in s.lower()) is None


def test_shipment_failure_email(mocker, mock_dag_run):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.google_scanning.email.send_email_with_server_name"
    )

    shipment_failure_email.function(
        "Failed to upload files to Google Drive: ['x.xml']",
        dag_run=mock_dag_run,
        params={"user_email": "staff@example.com"},
    )

    assert mock_send_email.called
    call_kwargs = mock_send_email.call_args[1]
    assert call_kwargs["subject"] == "Google Scanning On-Campus Shipment Failed"
    assert call_kwargs["to"] == ["devs@example.com", "staff@example.com"]

    html_body = BeautifulSoup(call_kwargs["html_content"], "html.parser")
    assert "Failed to upload files to Google Drive" in html_body.text
    assert html_body.find("a").text == "manual_2026-08-10"
    assert (
        html_body.find("a").attrs["href"]
        == "http://localhost:8080/dags/on_campus_shipment/runs/manual_2026-08-10"
    )


def test_send_shipment_failure_email_no_user_email(mocker, mock_dag_run):
    mock_send_email = mocker.patch(
        "libsys_airflow.plugins.google_scanning.email.send_email_with_server_name"
    )

    send_shipment_failure_email("boom", mock_dag_run, None)

    assert mock_send_email.call_args[1]["to"] == ["devs@example.com"]
    html_body = BeautifulSoup(
        mock_send_email.call_args[1]["html_content"], "html.parser"
    )
    assert "boom" in html_body.text
