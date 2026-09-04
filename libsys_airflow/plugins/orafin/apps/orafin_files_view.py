import pathlib

from fastapi import Depends, FastAPI, Request
from fastapi.responses import FileResponse

from libsys_airflow.plugins.shared.auth import require_view_access
from libsys_airflow.plugins.shared.utils import file_info, plugin_templates

app = FastAPI(
    dependencies=[Depends(require_view_access("Orafin Feeder-files and Reports"))]
)

templates = plugin_templates(pathlib.Path(__file__).resolve().parent.parent, "orafin")

files_base = pathlib.Path("/opt/airflow/orafin-files")


@app.get("/")
def orafin_files_home(request: Request):
    data = files_base / "data"
    reports = files_base / "reports"

    feeder_files = [
        file_info(feeder_file)
        for feeder_file in data.iterdir()
        if feeder_file.is_file()
    ]

    ap_reports = [file_info(report) for report in reports.iterdir() if report.is_file()]

    return templates.TemplateResponse(
        request,
        "index.html",
        {"feeder_files": feeder_files, "ap_reports": ap_reports},
    )


@app.get("/{type_of}/{file_name}")
def download(type_of: str, file_name: str):
    orafin_file_path = files_base / type_of / file_name
    media_type = "application/csv" if type_of != "data" else "application/text"
    return FileResponse(
        str(orafin_file_path),
        media_type=media_type,
        filename=file_name,
    )
