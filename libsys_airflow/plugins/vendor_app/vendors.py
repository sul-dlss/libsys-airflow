from folioclient import FolioClient
import logging
import requests

from airflow.models import Variable

from flask_appbuilder import expose, BaseView as AppBuilderBaseView


logger = logging.getLogger(__name__)


class VendorManagementView(AppBuilderBaseView):
    default_view = "vendors_index"
    route_base = "/vendors"

    @property
    def _folio_client(self):
        try:
            return FolioClient(
                Variable.get("okapi_url"),
                "sul",
                Variable.get("folio_user"),
                Variable.get("folio_password")
            )
        except ValueError as error:
            logger.error(error)
            raise

    def _get_vendors(self):
        """
        Returns vendors from FOLIO
        """
        vendors = ["AMALIV-SUL", "CASALI-SUL", "COUTTS-SUL", "HARRAS-SUL", "SFX", "YANKEE-SUL"]
        cql_query = f"({' or '.join(f'(code={vendor})' for vendor in vendors)})"
        vendor_result = requests.get(
            f"{self._folio_client.okapi_url}/organizations-storage/organizations?query={cql_query}",
            headers=self._folio_client.okapi_headers)
        vendor_result.raise_for_status()
        return vendor_result.json()

    @expose("/")
    def vendors_index(self):
        vendors = self._get_vendors()
        return self.render_template("index.html", vendors=vendors)
