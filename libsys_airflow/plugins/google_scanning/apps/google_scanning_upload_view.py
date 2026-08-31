import logging
import re

from datetime import date
from pathlib import Path

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from libsys_airflow.plugins.google_scanning.helpers import parse_barcodes
from libsys_airflow.plugins.google_scanning.staging import (
    archived_file_path,
    download_filename,
    list_shipped_carts,
    list_staged_carts,
    save_staged_file,
    trigger_on_campus_shipment_dag,
    trigger_stage_cart_items_dag,
)
from libsys_airflow.plugins.shared.csrf import CSRFCookieMiddleware, csrf_protect
from libsys_airflow.plugins.shared.utils import (
    plugin_templates,
    redirect_with_query_params,
)

logger = logging.getLogger(__name__)

app = FastAPI()
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
    barcode_file: UploadFile | None = File(default=None),  # noqa: B008
):
    if not cart_name.strip():
        return _render_home(request, error="Cart name is required.")
    if not barcode_file or not barcode_file.filename:
        return _render_home(request, error="A barcode file is required.")

    contents = barcode_file.file.read()
    try:
        text = contents.decode("utf-8")
    except UnicodeDecodeError:
        return _render_home(request, error="Barcode file must be plain text.")

    if not parse_barcodes(text):
        return _render_home(request, error="Barcode file is empty.")

    # Validate raw (unstripped) lines, not parse_barcodes' output -- a line
    # with leading/trailing whitespace must be rejected here rather than
    # silently trimmed and accepted as a valid barcode.
    raw_lines = [line for line in text.splitlines() if line.strip()]
    invalid = [line for line in raw_lines if not BARCODE_PATTERN.fullmatch(line)]
    if invalid:
        return _render_home(
            request,
            error=f"Barcode file contains invalid line(s): {', '.join(invalid[:5])}",
        )

    staged_file_path = save_staged_file(cart_name, barcode_file.filename, contents)

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
