from urllib.parse import unquote

import pytest  # noqa

from fastapi.testclient import TestClient

from libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view import (
    app,
)

client = TestClient(app, follow_redirects=False)


@pytest.fixture(autouse=True)
def mock_list_staged_carts(mocker):
    return mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.list_staged_carts",
        return_value=[
            {
                "cart_name": "cart-1",
                "filename": "barcodes.txt",
                "uploaded_at": "2026-01-01T00:00:00",
                "status": {"status": "Pending"},
            }
        ],
    )


def test_home_renders_staged_carts():
    response = client.get("/")

    assert response.status_code == 200
    assert "cart-1" in response.text
    assert "barcodes.txt" in response.text


def test_stage_cart_missing_cart_name():
    response = client.post(
        "/stage",
        data={"cart_name": " "},
        files={"barcode_file": ("barcodes.txt", b"12345\n", "text/plain")},
    )

    assert response.status_code == 200
    assert "Cart name is required." in response.text


def test_stage_cart_missing_file():
    response = client.post(
        "/stage",
        data={"cart_name": "cart-2"},
        files={"barcode_file": ("", b"", "text/plain")},
    )

    assert response.status_code == 200
    assert "A barcode file is required." in response.text


def test_stage_cart_success(mocker):
    mock_save = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.save_staged_file",
        return_value="/opt/airflow/data-export-files/google_scanning/staged/cart-2/barcodes.txt",
    )
    mock_trigger = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.trigger_stage_cart_items_dag",
        return_value="run-123",
    )

    response = client.post(
        "/stage",
        data={"cart_name": "cart-2"},
        files={"barcode_file": ("barcodes.txt", b"12345\n", "text/plain")},
    )

    assert response.status_code == 303
    assert "Staged cart-2" in unquote(response.headers["location"])
    mock_save.assert_called_once_with("cart-2", "barcodes.txt", b"12345\n")
    mock_trigger.assert_called_once()


def test_stage_cart_dag_trigger_failure(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.save_staged_file",
        return_value="/opt/airflow/data-export-files/google_scanning/staged/cart-2/barcodes.txt",
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.trigger_stage_cart_items_dag",
        side_effect=Exception("dag not found"),
    )

    response = client.post(
        "/stage",
        data={"cart_name": "cart-2"},
        files={"barcode_file": ("barcodes.txt", b"12345\n", "text/plain")},
    )

    assert response.status_code == 303
    assert "failed to start item processing" in unquote(response.headers["location"])


def test_ship_no_carts_selected():
    response = client.post("/ship", data={"user_email": "staff@example.com"})

    assert response.status_code == 200
    assert "Select at least one staged cart to ship." in response.text


def test_ship_success(mocker):
    mock_trigger = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.trigger_on_campus_shipment_dag",
        return_value="run-456",
    )

    response = client.post(
        "/ship",
        data={
            "selected_carts": ["cart-1/barcodes.txt"],
            "user_email": "staff@example.com",
        },
    )

    assert response.status_code == 303
    assert "run-456" in response.headers["location"]
    mock_trigger.assert_called_once_with(
        [{"cart_name": "cart-1", "filename": "barcodes.txt"}],
        "staff@example.com",
    )


def test_ship_dag_trigger_failure(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.trigger_on_campus_shipment_dag",
        side_effect=Exception("dag not found"),
    )

    response = client.post(
        "/ship",
        data={"selected_carts": ["cart-1/barcodes.txt"]},
    )

    assert response.status_code == 303
    assert "Failed to start shipment" in unquote(response.headers["location"])
