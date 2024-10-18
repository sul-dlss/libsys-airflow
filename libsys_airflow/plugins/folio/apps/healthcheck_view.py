import logging

from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from folioclient import FolioClient
from airflow.models import Variable
from flask import make_response

logger = logging.getLogger(__name__)


class Healthcheck(AppBuilderBaseView):
    default_view = "home"
    route_base = "/healthcheck"

    @expose("/")
    def home(self):
        folio_client = FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )
        statuses = self._statuses(folio_client)
        http_status = 200 if all(statuses.values()) else 500
        return make_response(
            self.render_template("healthcheck/index.html", statuses=statuses),
            http_status,
        )

    def _statuses(self, folio_client):
        return {
            "Folio login": self._check_folio_login(folio_client),
            "Holdings custom mappings": self._check_holdings_custom_mappings(
                folio_client
            ),
            "Bib custom mappings": self._check_bib_custom_mappings(folio_client),
        }

    def _check_folio_login(self, folio_client):
        try:
            folio_client
            return True
        except Exception:
            return False

    def _check_holdings_custom_mappings(self, folio_client):
        mapping_rules = folio_client.folio_get("/mapping-rules/marc-holdings")
        entities = mapping_rules['852'][0]['entity']
        matching_entities = [
            entity
            for entity in entities
            if entity['target'] == 'permanentLocationId'
            and entity['subfield'] == ['b', 'c']
        ]
        return len(matching_entities) > 0

    def _check_bib_custom_mappings(self, folio_client):
        mapping_rules = folio_client.folio_get("/mapping-rules/marc-bib")
        return '910' in mapping_rules
