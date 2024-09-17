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
        statuses = self._statuses
        http_status = 200 if all(statuses.values()) else 500
        return make_response(
            self.render_template("healthcheck/index.html", statuses=statuses),
            http_status,
        )

    @property
    def _statuses(self):
        if not self._check_folio_login():
            return {"Folio login": False}

        return {
            "Folio login": True,
            "Migration login": self._check_migration_login(),
            "Holdings custom mappings": self._check_holdings_custom_mappings(),
            "Bib custom mappings": self._check_bib_custom_mappings(),
        }

    @property
    def _folio_client(self):
        return FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )

    def _check_folio_login(self):
        try:
            self._folio_client
            return True
        except Exception:
            return False

    def _check_holdings_custom_mappings(self):
        mapping_rules = self._folio_client.folio_get("/mapping-rules/marc-holdings")
        entities = mapping_rules['852'][0]['entity']
        matching_entities = [
            entity
            for entity in entities
            if entity['target'] == 'permanentLocationId'
            and entity['subfield'] == ['b', 'c']
        ]
        return len(matching_entities) > 0

    def _check_bib_custom_mappings(self):
        mapping_rules = self._folio_client.folio_get("/mapping-rules/marc-bib")
        return '910' in mapping_rules

    def _check_migration_login(self):
        try:
            migration_client = FolioClient(
                Variable.get("OKAPI_URL"),
                "sul",
                Variable.get("FOLIO_USER"),
                Variable.get("FOLIO_PASSWORD"),
            )
            return migration_client is not None
        except Exception:
            return False
