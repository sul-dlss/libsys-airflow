import json
import pathlib

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse

from libsys_airflow.plugins.shared.utils import plugin_templates

app = FastAPI()

templates = plugin_templates(
    pathlib.Path(__file__).resolve().parent.parent, "data-export-download"
)

parent = pathlib.Path(__file__).resolve().parent
vendor_file = open(parent / "vendors.json")
vendors = json.load(vendor_file)

files_base = pathlib.Path("/opt/airflow/data-export-files")


@app.get("/")
def data_export_download_home(request: Request):
    content = []
    for vendor in vendors["vendors"]:
        for state in ["marc-files", "transmitted"]:
            for kind in ["new", "updates", "deletes"]:
                for path in (files_base / vendor / state / kind).glob("*"):
                    content.append(
                        {
                            "vendor": vendor,
                            "state": state,
                            "kind": kind,
                            "filename": path.name,
                        }
                    )

    return templates.TemplateResponse(request, "index.html", {"content": content})


@app.get("/downloads/{vendor}/{state}/{folder}/{filename}")
def vendor_marc_record(vendor: str, state: str, folder: str, filename: str):
    download_name = f"{vendor}-{state}-{folder}-{filename}"
    return FileResponse(
        str(files_base / vendor / state / folder / filename),
        media_type="application/marc",
        filename=download_name,
    )
