import pathlib

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

app = FastAPI()

templates = Jinja2Templates(
    directory=pathlib.Path(__file__).resolve().parent.parent
    / "templates"
    / "data-export-oclc-reports"
)

files_base = pathlib.Path("/opt/airflow/data-export-files")

LOOKUP_LIBRARY_CODE = {
    "CASUM": "Lane Medical Library",
    "HIN": "Hoover Institution Library and Archives",
    "RCJ": "Robert Crown Law Library",
    "S7Z": "Graduate School of Business",
    "STF": "Stanford University Libraries",
}

LOOKUP_REPORT_NAME = {
    "match": "Match BIB Record Errors",
    "multiple_oclc_numbers": "Multiple OCLC Numbers",
    "new_marc_errors": "New OCLC MARC Record Errors",
    "set_holdings": "Set OCLC Holdings Errors",
    "set_holdings_match": "Set OCLC Holdings Match Errors",
    "unset_holdings": "Unset (delete) OCLC Holdings Errors",
}


@app.get("/")
def data_export_oclc_reports_home(request: Request):
    oclc_reports_home = files_base / "oclc" / "reports"
    libraries: dict[str, dict] = {}
    no_holdings: list = []
    for library in oclc_reports_home.iterdir():
        if library.name.startswith("missing_holdings"):
            no_holdings = [report for report in library.glob("*.html")]
            continue
        if not library.is_dir():
            continue
        libraries[library.name] = {
            "name": LOOKUP_LIBRARY_CODE[library.name],
        }

        for report_type in library.iterdir():
            if not report_type.is_dir():
                continue
            libraries[library.name][report_type.name] = {
                "name": LOOKUP_REPORT_NAME[report_type.name],
                "reports": [],
            }
            for report in report_type.glob("*.html"):
                libraries[library.name][report_type.name]["reports"].append(report)

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "libraries": libraries,
            "sortlibs": sorted(libraries, key=lambda x: (libraries[x]['name'])),
            "no_holdings_instances": sorted(no_holdings),
        },
    )


@app.get("/{library_code}/{report_type}/{report_name}")
def oclc_report(
    library_code: str, report_type: str, report_name: str, request: Request
):
    report_path = (
        files_base / "oclc" / "reports" / library_code / report_type / report_name
    )

    return templates.TemplateResponse(
        request,
        "report.html",
        {
            "library_name": LOOKUP_LIBRARY_CODE[library_code],
            "contents": report_path.read_text(),
        },
    )


@app.get("/missing_holdings/{report_name}")
def oclc_missing_holdings(report_name: str, request: Request):
    report_path = files_base / "oclc" / "reports" / "missing_holdings" / report_name

    return templates.TemplateResponse(
        request,
        "report.html",
        {"library_name": "All Libraries", "contents": report_path.read_text()},
    )
