from urllib.parse import unquote_plus

import pytest  # noqa

from fastapi.testclient import TestClient
from csrf_helpers import csrf_test_client, token_from_cookie  # noqa

from libsys_airflow.plugins.shared.csrf import CSRF_COOKIE_NAME, CSRF_FIELD_NAME

from libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view import (
    app,
)

client = csrf_test_client(app, follow_redirects=False)


@pytest.fixture(autouse=True)
def mock_list_staged_carts(mocker):
    return mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.list_staged_carts",
        return_value=[
            {
                "cart_name": "cart-1",
                "filename": "barcodes.txt",
                "uploaded_at": "2026-01-01T00:00:00",
                "status": {"status": "staged"},
            }
        ],
    )


@pytest.fixture(autouse=True)
def mock_list_shipped_carts(mocker):
    return mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.list_shipped_carts",
        return_value=[],
    )


def test_home_renders_staged_carts():
    response = client.get("/")

    assert response.status_code == 200
    assert "cart-1" in response.text
    assert "barcodes.txt" in response.text
    assert "Staged" in response.text


def test_home_renders_refresh_button():
    response = client.get("/")

    assert response.status_code == 200
    assert 'id="refresh-tables"' in response.text
    assert "window.location.reload()" in response.text


def test_home_renders_barcode_counts_for_staged_cart(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.list_staged_carts",
        return_value=[
            {
                "cart_name": "cart-1",
                "filename": "barcodes.txt",
                "uploaded_at": "2026-01-01T00:00:00",
                "status": {
                    "status": "failed",
                    "total_barcodes": 15,
                    "updated": 0,
                    "missing_barcodes": [str(n) for n in range(15)],
                    "errors": [],
                },
            }
        ],
    )

    response = client.get("/")

    assert response.status_code == 200
    assert "0/15 updated, 15 missing" in response.text


def test_home_renders_singular_error_count(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.list_staged_carts",
        return_value=[
            {
                "cart_name": "cart-1",
                "filename": "barcodes.txt",
                "uploaded_at": "2026-01-01T00:00:00",
                "status": {
                    "status": "staged",
                    "total_barcodes": 2,
                    "updated": 1,
                    "missing_barcodes": [],
                    "errors": [{"barcode": "1", "reason": "boom"}],
                },
            }
        ],
    )

    response = client.get("/")

    assert response.status_code == 200
    assert "1/2 updated, 1 error" in response.text
    assert "1 errors" not in response.text


def test_home_omits_counts_when_status_has_no_totals():
    response = client.get("/")

    assert response.status_code == 200
    assert "updated" not in response.text


def test_home_renders_shared_table_search(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.list_shipped_carts",
        return_value=[
            {
                "cart_name": "cart-3",
                "filename": "barcodes.txt",
                "download_filename": "barcodes.csv",
                "shipped_at": "20260807",
                "status": {"status": "shipped"},
            }
        ],
    )

    response = client.get("/")

    assert response.status_code == 200
    assert 'id="table-search"' in response.text
    # both the staged and shipped rows opt into the shared filter
    assert response.text.count('class="filterable-row"') == 2


def test_home_renders_unknown_status_when_status_missing(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.list_staged_carts",
        return_value=[
            {
                "cart_name": "cart-1",
                "filename": "barcodes.txt",
                "uploaded_at": "2026-01-01T00:00:00",
                "status": {},
            }
        ],
    )

    response = client.get("/")

    assert response.status_code == 200
    assert "Unknown" in response.text


def test_home_renders_shipped_carts(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.list_shipped_carts",
        return_value=[
            {
                "cart_name": "cart-3",
                "filename": "barcodes.txt",
                "download_filename": "barcodes.csv",
                "shipped_at": "20260807",
                "status": {"status": "shipped"},
            }
        ],
    )

    response = client.get("/")

    assert response.status_code == 200
    assert "cart-3" in response.text
    assert "20260807" in response.text
    assert "Shipped" in response.text
    assert 'href="download/cart-3/barcodes.txt"' in response.text
    assert ">barcodes.csv</a>" in response.text


def test_home_renders_unknown_status_for_shipped_cart_missing_status(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.list_shipped_carts",
        return_value=[
            {
                "cart_name": "cart-3",
                "filename": "barcodes.txt",
                "download_filename": "barcodes.csv",
                "shipped_at": None,
                "status": {},
            }
        ],
    )

    response = client.get("/")

    assert response.status_code == 200
    assert "cart-3" in response.text
    assert response.text.count("Unknown") == 2


def test_home_renders_success_from_redirect_query_param():
    response = client.get("/", params={"success": "Staged cart-2."})

    assert response.status_code == 200
    assert "Staged cart-2." in response.text
    assert 'alert-success">Staged cart-2.' in response.text


def test_home_renders_warning_from_redirect_query_param():
    response = client.get("/", params={"warning": "Failed to start item processing."})

    assert response.status_code == 200
    assert 'alert-warning">Failed to start item processing.' in response.text


def test_home_renders_error_from_redirect_query_param():
    response = client.get("/", params={"error": "Cart name is required."})

    assert response.status_code == 200
    assert 'alert-error">Cart name is required.' in response.text


def test_home_renders_shipped_at_defaulting_to_today(mocker):
    mock_date = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.date"
    )
    mock_date.today.return_value.isoformat.return_value = "2026-08-07"

    response = client.get("/")

    assert response.status_code == 200
    assert 'id="shipped_at" name="shipped_at" value="2026-08-07"' in response.text


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


def test_stage_cart_empty_file():
    response = client.post(
        "/stage",
        data={"cart_name": "cart-2"},
        files={"barcode_file": ("barcodes.txt", b"\n   \n", "text/plain")},
    )

    assert response.status_code == 200
    assert "Barcode file is empty." in response.text


def test_stage_cart_non_utf8_file():
    response = client.post(
        "/stage",
        data={"cart_name": "cart-2"},
        files={"barcode_file": ("barcodes.txt", b"\xff\xfe\x00\x01", "text/plain")},
    )

    assert response.status_code == 200
    assert "Barcode file must be plain text." in response.text


def test_stage_cart_rejects_barcode_with_spaces():
    response = client.post(
        "/stage",
        data={"cart_name": "cart-2"},
        files={"barcode_file": ("barcodes.txt", b"36105 061323494\n", "text/plain")},
    )

    assert response.status_code == 200
    assert "Barcode file contains invalid line(s): 36105 061323494" in response.text


def test_stage_cart_rejects_barcode_with_leading_or_trailing_whitespace():
    response = client.post(
        "/stage",
        data={"cart_name": "cart-2"},
        files={"barcode_file": ("barcodes.txt", b"  36105061323494  \n", "text/plain")},
    )

    assert response.status_code == 200
    assert "Barcode file contains invalid line(s):" in response.text
    assert "  36105061323494  " in response.text


def test_stage_cart_accepts_alphanumeric_and_dash_barcodes(mocker):
    mock_save = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.save_staged_file",
        return_value="/opt/airflow/data-export-files/google_scanning/staged/cart-2/barcodes.txt",
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.trigger_stage_cart_items_dag",
        return_value="run-123",
    )

    contents = b"001AMT2225\n5108203-3001\n36105061323494\n"
    response = client.post(
        "/stage",
        data={"cart_name": "cart-2"},
        files={"barcode_file": ("barcodes.txt", contents, "text/plain")},
    )

    assert response.status_code == 303
    mock_save.assert_called_once_with("cart-2", "barcodes.txt", contents)


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
    assert "Staged cart-2" in unquote_plus(response.headers["location"])
    mock_save.assert_called_once_with("cart-2", "barcodes.txt", b"12345\n")
    mock_trigger.assert_called_once()

    followed = client.get(response.headers["location"])
    assert 'alert-success">Staged cart-2.' in followed.text


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
    assert "warning=" in response.headers["location"]
    assert "failed to start item processing" in unquote_plus(
        response.headers["location"]
    )

    followed = client.get(response.headers["location"])
    assert "alert-warning" in followed.text


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
            "shipped_at": "2026-08-06",
        },
    )

    assert response.status_code == 303
    assert "success=" in response.headers["location"]
    assert "run-456" in response.headers["location"]
    mock_trigger.assert_called_once_with(
        [{"cart_name": "cart-1", "filename": "barcodes.txt"}],
        "staff@example.com",
        "2026-08-06",
    )

    followed = client.get(response.headers["location"])
    assert "alert-success" in followed.text


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
    assert "warning=" in response.headers["location"]
    assert "Failed to start shipment" in unquote_plus(response.headers["location"])

    followed = client.get(response.headers["location"])
    assert "alert-warning" in followed.text


def test_download_shipped_file(mocker, tmp_path):
    file_path = tmp_path / "barcodes.txt"
    file_path.write_bytes(b"12345\n67890\n")
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.archived_file_path",
        return_value=file_path,
    )

    response = client.get("/download/cart-1/barcodes.txt")

    assert response.status_code == 200
    assert response.content == b"12345\n67890\n"
    assert 'filename="barcodes.csv"' in response.headers["content-disposition"]


def test_download_shipped_file_invalid_path(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.archived_file_path",
        side_effect=ValueError("Invalid archived file path"),
    )

    response = client.get("/download/cart-1/barcodes.txt")

    assert response.status_code == 404


def test_download_shipped_file_missing(mocker, tmp_path):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.archived_file_path",
        return_value=tmp_path / "does-not-exist.txt",
    )

    response = client.get("/download/cart-1/does-not-exist.txt")

    assert response.status_code == 404


def test_home_renders_csrf_field():
    fresh_client = TestClient(app, follow_redirects=False)

    response = fresh_client.get("/")

    token = token_from_cookie(response.cookies[CSRF_COOKIE_NAME])
    assert f'<input type="hidden" name="csrf_token" value="{token}">' in response.text
    # Both the stage and the ship form carry the token
    assert response.text.count('name="csrf_token"') == 2


def test_stage_cart_with_csrf_token_from_the_form(mocker):
    """The path a browser takes: token issued in the cookie, submitted in the form."""
    mock_save = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.save_staged_file",
        return_value="/opt/airflow/data-export-files/google_scanning/staged/cart-2/barcodes.txt",
    )
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.trigger_stage_cart_items_dag",
        return_value="run-123",
    )
    fresh_client = TestClient(app, follow_redirects=False)
    fresh_client.get("/")
    token = token_from_cookie(fresh_client.cookies[CSRF_COOKIE_NAME])

    response = fresh_client.post(
        "/stage",
        data={"cart_name": "cart-2", CSRF_FIELD_NAME: token},
        files={"barcode_file": ("barcodes.txt", b"12345\n", "text/plain")},
    )

    assert response.status_code == 303
    mock_save.assert_called_once_with("cart-2", "barcodes.txt", b"12345\n")


def test_stage_cart_without_csrf_token(mocker):
    mock_save = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.save_staged_file"
    )

    response = TestClient(app, follow_redirects=False).post(
        "/stage",
        data={"cart_name": "cart-2"},
        files={"barcode_file": ("barcodes.txt", b"12345\n", "text/plain")},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "CSRF token missing or invalid"
    mock_save.assert_not_called()


def test_stage_cart_with_mismatched_csrf_token(mocker):
    mock_save = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.save_staged_file"
    )
    fresh_client = TestClient(app, follow_redirects=False)
    fresh_client.get("/")

    response = fresh_client.post(
        "/stage",
        data={"cart_name": "cart-2", CSRF_FIELD_NAME: "not-the-issued-token"},
        files={"barcode_file": ("barcodes.txt", b"12345\n", "text/plain")},
    )

    assert response.status_code == 403
    mock_save.assert_not_called()


def test_trigger_shipment_with_csrf_token_from_the_form(mocker):
    mock_trigger = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.trigger_on_campus_shipment_dag",
        return_value="run-456",
    )
    fresh_client = TestClient(app, follow_redirects=False)
    fresh_client.get("/")
    token = token_from_cookie(fresh_client.cookies[CSRF_COOKIE_NAME])

    response = fresh_client.post(
        "/ship",
        data={"selected_carts": ["cart-1/barcodes.txt"], CSRF_FIELD_NAME: token},
    )

    assert response.status_code == 303
    mock_trigger.assert_called_once()


def test_trigger_shipment_without_csrf_token(mocker):
    mock_trigger = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.trigger_on_campus_shipment_dag"
    )

    response = TestClient(app, follow_redirects=False).post(
        "/ship", data={"selected_carts": ["cart-1/barcodes.txt"]}
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "CSRF token missing or invalid"
    mock_trigger.assert_not_called()
