import datetime
import pathlib

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()

templates = Jinja2Templates(
    directory=[
        pathlib.Path(__file__).resolve().parent.parent.parent / "templates",
        pathlib.Path(__file__).resolve().parent.parent / "templates" / "orafin",
    ]
)

files_base = pathlib.Path("/opt/airflow/orafin-files")


def _file_info(file: pathlib.Path) -> dict:
    stats = file.stat()
    created_date = datetime.datetime.fromtimestamp(stats.st_ctime)
    return {
        "name": file.name,
        "date_created": created_date.isoformat(),
        "size": f"{stats.st_size:,}",
    }


@app.get("/")
def orafin_files_home(request: Request):
    data = files_base / "data"
    reports = files_base / "reports"

    feeder_files = [
        _file_info(feeder_file)
        for feeder_file in data.iterdir()
        if feeder_file.is_file()
    ]

    ap_reports = [
        _file_info(report) for report in reports.iterdir() if report.is_file()
    ]

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
