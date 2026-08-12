import datetime
import pathlib

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()

templates = Jinja2Templates(
    directory=[
        pathlib.Path(__file__).resolve().parent.parent.parent / "templates",
        pathlib.Path(__file__).resolve().parent.parent / "templates" / "sdr",
    ]
)

reports_base = pathlib.Path("/opt/airflow/sdr-files/reports")


def _file_info(file: pathlib.Path) -> dict:
    stats = file.stat()
    created_date = datetime.datetime.fromtimestamp(stats.st_ctime)
    return {
        "name": file.name,
        "date_created": created_date.isoformat(),
        "size": f"{stats.st_size:,}",
    }


@app.get("/")
def sdr_missing_barcodes_home(request: Request):
    missing_barcodes_files = [_file_info(row) for row in reports_base.glob("*.csv")]
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
