import logging
import pathlib

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

from folioclient import FolioClient
from airflow.sdk import Variable

logger = logging.getLogger(__name__)

app = FastAPI()

templates = Jinja2Templates(
    directory=pathlib.Path(__file__).resolve().parent.parent / "templates"
)


def _check_folio_login(folio_client):
    try:
        folio_client
        return True
    except Exception:
        return False


def _check_holdings_custom_mappings(folio_client):
    mapping_rules = folio_client.folio_get("/mapping-rules/marc-holdings")
    entities = mapping_rules['852'][0]['entity']
    matching_entities = [
        entity
        for entity in entities
        if entity['target'] == 'permanentLocationId'
        and entity['subfield'] == ['b', 'c']
    ]
    return len(matching_entities) > 0


def _check_bib_custom_mappings(folio_client):
    mapping_rules = folio_client.folio_get("/mapping-rules/marc-bib")
    return '910' in mapping_rules


def _statuses(folio_client):
    return {
        "Folio login": _check_folio_login(folio_client),
        "Holdings custom mappings": _check_holdings_custom_mappings(folio_client),
        "Bib custom mappings": _check_bib_custom_mappings(folio_client),
    }


@app.get("/")
def home(request: Request):
    client = FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )
    statuses = _statuses(client)
    http_status = 200 if all(statuses.values()) else 500
    return templates.TemplateResponse(
        request,
        "healthcheck/index.html",
        {"statuses": statuses},
        status_code=http_status,
    )
