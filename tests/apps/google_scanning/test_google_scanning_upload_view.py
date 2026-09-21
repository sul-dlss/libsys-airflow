from urllib.parse import unquote_plus

import pytest  # noqa

from fastapi.testclient import TestClient
from auth_helpers import authenticated_app_fixture  # noqa
from csrf_helpers import csrf_test_client, token_from_cookie  # noqa

from libsys_airflow.plugins.shared.csrf import CSRF_COOKIE_NAME, CSRF_FIELD_NAME

from libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view import (
    app,
)

authenticated = authenticated_app_fixture(app)
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


def test_home_renders_barcode_textarea():
    response = client.get("/")

    assert response.status_code == 200
    assert '<textarea id="barcodes" name="barcodes"' in response.text
    assert 'name="source_filename"' in response.text


def test_home_file_input_is_not_submitted():
    """
    The picker is browser-side sugar that appends into the textarea. Without
    a name attribute it cannot post a file part, which is what keeps /stage
    to a single text input to validate.
    """
    response = client.get("/")

    assert 'id="barcode_file"' in response.text
    assert 'name="barcode_file"' not in response.text


def test_home_renders_drop_handling_script():
    response = client.get("/")

    assert 'addEventListener("drop"' in response.text
    # the client-side mirror of the route's checks
    assert "TextDecoder" in response.text
    assert "/^[A-Za-z0-9-]+$/" in response.text


@pytest.fixture
def mock_stage_cart(mocker):
    """Stubs out the filesystem write and the DAG trigger for a /stage POST."""
    return {
        "save": mocker.patch(
            "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.save_staged_file",
            return_value="/opt/airflow/data-export-files/google_scanning/staged/cart-2/barcodes.txt",
        ),
        "trigger": mocker.patch(
            "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.trigger_stage_cart_items_dag",
            return_value="run-123",
        ),
    }


def test_stage_cart_missing_cart_name():
    response = client.post("/stage", data={"cart_name": " ", "barcodes": "12345"})

    assert response.status_code == 200
    assert "Cart name is required." in response.text


def test_stage_cart_missing_barcodes():
    response = client.post("/stage", data={"cart_name": "cart-2"})

    assert response.status_code == 200
    assert "Enter or drop in at least one barcode." in response.text


def test_stage_cart_blank_barcodes():
    response = client.post(
        "/stage", data={"cart_name": "cart-2", "barcodes": "\n   \n"}
    )

    assert response.status_code == 200
    assert "Enter or drop in at least one barcode." in response.text


def test_stage_cart_rejects_barcode_with_spaces():
    response = client.post(
        "/stage", data={"cart_name": "cart-2", "barcodes": "36105 061323494\n"}
    )

    assert response.status_code == 200
    assert "Barcode list contains invalid line(s): 36105 061323494" in response.text


def test_stage_cart_rejects_barcode_with_leading_or_trailing_whitespace():
    response = client.post(
        "/stage", data={"cart_name": "cart-2", "barcodes": "  36105061323494  \n"}
    )

    assert response.status_code == 200
    assert "Barcode list contains invalid line(s):" in response.text
    assert "  36105061323494  " in response.text


def test_stage_cart_redisplays_submitted_values_on_error():
    """
    A rejected list has to come back in the form -- staff may have dropped in
    hundreds of barcodes and should not have to reassemble them.
    """
    response = client.post(
        "/stage",
        data={"cart_name": "cart-2", "barcodes": "12345\nbad barcode\n67890"},
    )

    assert response.status_code == 200
    assert 'name="cart_name" value="cart-2"' in response.text
    assert "12345\nbad barcode\n67890" in response.text


def test_stage_cart_accepts_alphanumeric_and_dash_barcodes(mock_stage_cart):
    barcodes = "001AMT2225\n5108203-3001\n36105061323494\n"

    response = client.post(
        "/stage",
        data={
            "cart_name": "cart-2",
            "barcodes": barcodes,
            "source_filename": "barcodes.txt",
        },
    )

    assert response.status_code == 303
    mock_stage_cart["save"].assert_called_once_with(
        "cart-2", "barcodes.txt", barcodes.encode("utf-8")
    )


def test_stage_cart_keeps_the_dropped_files_name(mock_stage_cart):
    client.post(
        "/stage",
        data={
            "cart_name": "cart-2",
            "barcodes": "12345\n",
            "source_filename": "cart-2-shelflist.txt",
        },
    )

    assert mock_stage_cart["save"].call_args.args[1] == "cart-2-shelflist.txt"


def test_stage_cart_names_typed_barcodes_after_the_cart(mock_stage_cart):
    """No file was dropped, so there is no filename to carry over."""
    client.post("/stage", data={"cart_name": "cart-2", "barcodes": "12345\n"})

    assert mock_stage_cart["save"].call_args.args[1] == "cart-2.txt"


def test_stage_cart_sanitizes_the_submitted_filename(mock_stage_cart):
    """
    source_filename is a plain form field now, so a caller can put anything
    in it; it must not be able to escape the cart directory.
    """
    client.post(
        "/stage",
        data={
            "cart_name": "cart-2",
            "barcodes": "12345\n",
            "source_filename": "../../../../tmp/evil.txt",
        },
    )

    assert mock_stage_cart["save"].call_args.args[1] == "evil.txt"


def test_stage_cart_rejects_a_traversing_cart_name(mocker):
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.save_staged_file",
        side_effect=ValueError("Invalid cart name"),
    )
    mock_trigger = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.trigger_stage_cart_items_dag"
    )

    response = client.post(
        "/stage", data={"cart_name": "../../etc", "barcodes": "12345\n"}
    )

    assert response.status_code == 200
    assert "Cart name is not valid." in response.text
    mock_trigger.assert_not_called()


def test_stage_cart_normalizes_textarea_line_endings(mock_stage_cart):
    """Browsers submit textarea content with CRLF; the stored file should not."""
    client.post(
        "/stage",
        data={"cart_name": "cart-2", "barcodes": "12345\r\n67890\r\n"},
    )

    assert mock_stage_cart["save"].call_args.args[2] == b"12345\n67890\n"


def test_stage_cart_success(mock_stage_cart):
    response = client.post(
        "/stage",
        data={
            "cart_name": "cart-2",
            "barcodes": "12345\n",
            "source_filename": "barcodes.txt",
        },
    )

    assert response.status_code == 303
    assert "Staged cart-2" in unquote_plus(response.headers["location"])
    mock_stage_cart["save"].assert_called_once_with(
        "cart-2", "barcodes.txt", b"12345\n"
    )
    mock_stage_cart["trigger"].assert_called_once()

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
        "/stage", data={"cart_name": "cart-2", "barcodes": "12345\n"}
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
        data={
            "cart_name": "cart-2",
            "barcodes": "12345\n",
            "source_filename": "barcodes.txt",
            CSRF_FIELD_NAME: token,
        },
    )

    assert response.status_code == 303
    mock_save.assert_called_once_with("cart-2", "barcodes.txt", b"12345\n")


def test_stage_cart_without_csrf_token(mocker):
    mock_save = mocker.patch(
        "libsys_airflow.plugins.google_scanning.apps.google_scanning_upload_view.save_staged_file"
    )

    response = TestClient(app, follow_redirects=False).post(
        "/stage",
        data={"cart_name": "cart-2", "barcodes": "12345\n"},
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
        data={
            "cart_name": "cart-2",
            "barcodes": "12345\n",
            CSRF_FIELD_NAME: "not-the-issued-token",
        },
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
