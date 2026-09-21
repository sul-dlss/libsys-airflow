import logging
import re

from datetime import date
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse

from libsys_airflow.plugins.google_scanning.helpers import parse_barcodes
from libsys_airflow.plugins.google_scanning.staging import (
    archived_file_path,
    download_filename,
    list_shipped_carts,
    list_staged_carts,
    safe_staged_filename,
    save_staged_file,
    trigger_on_campus_shipment_dag,
    trigger_stage_cart_items_dag,
)
from libsys_airflow.plugins.shared.auth import require_view_access
from libsys_airflow.plugins.shared.csrf import CSRFCookieMiddleware, csrf_protect
from libsys_airflow.plugins.shared.utils import (
    plugin_templates,
    redirect_with_query_params,
)

logger = logging.getLogger(__name__)

app = FastAPI(
    openapi_url=None,
    dependencies=[Depends(require_view_access("Google Scanning Upload"))],
)
app.add_middleware(CSRFCookieMiddleware)

BARCODE_PATTERN = re.compile(r"^[A-Za-z0-9-]+$")

templates = plugin_templates(
    Path(__file__).resolve().parent.parent, "google_scanning_upload"
)


def _render_home(
    request: Request,
    error: str | None = None,
    warning: str | None = None,
    success: str | None = None,
    form_values: dict | None = None,
):
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "staged_carts": list_staged_carts(),
            "shipped_carts": list_shipped_carts(),
            "error": error,
            "warning": warning,
            "success": success,
            # Barcode lists run to hundreds of lines, so a rejected submission
            # has to come back with the staff member's input still in the form
            # rather than making them paste or drop it all again.
            "form_values": form_values or {},
            "today": date.today().isoformat(),
        },
    )


@app.get("/")
def home(request: Request):
    return _render_home(
        request,
        error=request.query_params.get("error"),
        warning=request.query_params.get("warning"),
        success=request.query_params.get("success"),
    )


def _redirect_home(**query_params):
    return redirect_with_query_params(".", **query_params)


@app.post("/stage", dependencies=[Depends(csrf_protect)])
def stage_cart(
    request: Request,
    cart_name: str = Form(...),  # noqa: B008
    barcodes: str = Form(default=""),  # noqa: B008
    source_filename: str = Form(default=""),  # noqa: B008
):
    """
    Stages a cart from the barcodes textarea.

    Nothing is uploaded: the browser reads any picked or dropped file, runs
    the same checks applied below, and appends its contents to the textarea,
    so this route always receives plain text. source_filename carries the
    name of the first file that was added, purely so the staged file (and
    the CSV it is downloaded as) keeps the name staff recognise; it is
    advisory and gets sanitized before use.

    The checks are still enforced here rather than trusted from the browser,
    since the form's JavaScript is a convenience and the POST is reachable
    without it.
    """
    form_values = {"cart_name": cart_name, "barcodes": barcodes}

    if not cart_name.strip():
        return _render_home(
            request, error="Cart name is required.", form_values=form_values
        )

    # Textareas submit CRLF line endings; normalize so the stored file and
    # everything downstream of it sees the same single-newline barcode list
    # a directly uploaded file would have produced.
    text = barcodes.replace("\r\n", "\n").replace("\r", "\n")

    if not parse_barcodes(text):
        return _render_home(
            request,
            error="Enter or drop in at least one barcode.",
            form_values=form_values,
        )

    # Validate raw (unstripped) lines, not parse_barcodes' output -- a line
    # with leading/trailing whitespace must be rejected here rather than
    # silently trimmed and accepted as a valid barcode.
    raw_lines = [line for line in text.splitlines() if line.strip()]
    invalid = [line for line in raw_lines if not BARCODE_PATTERN.fullmatch(line)]
    if invalid:
        return _render_home(
            request,
            error=f"Barcode list contains invalid line(s): {', '.join(invalid[:5])}",
            form_values=form_values,
        )

    filename = safe_staged_filename(source_filename, cart_name)
    try:
        staged_file_path = save_staged_file(cart_name, filename, text.encode("utf-8"))
    except ValueError:
        return _render_home(
            request, error="Cart name is not valid.", form_values=form_values
        )

    try:
        trigger_stage_cart_items_dag(str(staged_file_path), cart_name)
    except Exception as e:
        logger.error(f"Error triggering {cart_name} staging DAG run: {e}")
        return _redirect_home(
            warning=f"Staged {cart_name}, but failed to start item processing."
        )

    return _redirect_home(success=f"Staged {cart_name}.")


@app.post("/ship", dependencies=[Depends(csrf_protect)])
def trigger_shipment(
    request: Request,
    selected_carts: list[str] = Form(default=[]),  # noqa: B008
    user_email: str | None = Form(default=None),  # noqa: B008
    shipped_at: str = Form(default=""),  # noqa: B008
):
    if not selected_carts:
        return _render_home(request, error="Select at least one staged cart to ship.")

    carts = []
    for selected_cart in selected_carts:
        cart_name, _, filename = selected_cart.partition("/")
        carts.append({"cart_name": cart_name, "filename": filename})

    shipped_at = shipped_at or date.today().isoformat()

    try:
        dag_run_id = trigger_on_campus_shipment_dag(carts, user_email, shipped_at)
    except Exception as e:
        logger.error(f"Error triggering on-campus shipment DAG run: {e}")
        return _redirect_home(warning="Failed to start shipment.")

    return _redirect_home(success=f"Started shipment DAG run {dag_run_id}.")


@app.get("/download/{cart_name}/{filename}")
async def download_shipped_file(cart_name: str, filename: str):
    try:
        file_path = archived_file_path(cart_name, filename)
    except ValueError:
        raise HTTPException(status_code=404, detail="File not found")

    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(file_path, filename=download_filename(filename))
