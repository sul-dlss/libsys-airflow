import pathlib

from fastapi import Depends, FastAPI, Request
from fastapi.responses import FileResponse

from libsys_airflow.plugins.shared.auth import require_view_access
from libsys_airflow.plugins.shared.utils import file_info, plugin_templates

app = FastAPI(
    dependencies=[Depends(require_view_access("SDR Missing Barcodes Reports"))]
)

templates = plugin_templates(pathlib.Path(__file__).resolve().parent.parent, "sdr")

reports_base = pathlib.Path("/opt/airflow/sdr-files/reports")


@app.get("/")
def sdr_missing_barcodes_home(request: Request):
    missing_barcodes_files = [file_info(row) for row in reports_base.glob("*.csv")]
    return templates.TemplateResponse(
        request,
        "index.html",
        {"missing_barcodes_files": missing_barcodes_files},
    )


@app.get("/{file_name}")
def download(file_name: str):
    report_path = reports_base / file_name
    return FileResponse(
        str(report_path),
        media_type="application/csv",
        filename=file_name,
    )
