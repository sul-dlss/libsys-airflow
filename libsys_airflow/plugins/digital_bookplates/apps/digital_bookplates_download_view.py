import pathlib

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates

app = FastAPI()

templates = Jinja2Templates(
    directory=pathlib.Path(__file__).resolve().parent.parent
    / "templates"
    / "digital_bookplates_download"
)

files_base = pathlib.Path("/opt/airflow/digital-bookplates")


@app.get("/")
def digital_bookplates_download_home(request: Request):
    content = []
    for path in files_base.rglob("*.csv"):
        content.append(
            {
                "date": f"{path.parent.parent.parent.name}-{path.parent.parent.name}-{path.parent.name}",
                "year": path.parent.parent.parent.name,
                "month": path.parent.parent.name,
                "day": path.parent.name,
                "filename": path.name,
            }
        )

    return templates.TemplateResponse(request, "index.html", {"content": content})


@app.get("/{year}/{month}/{day}/{filename}")
def csv_file(year: str, month: str, day: str, filename: str):
    folder_file = f"{year}-{month}-{day}-{filename}"
    return FileResponse(
        str(files_base / year / month / day / filename),
        media_type="application/csv",
        filename=folder_file,
    )
