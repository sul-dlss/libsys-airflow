import requests

from airflow.models import Variable

from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from flask import flash, request, redirect, Response
from folioclient import FolioClient

class VendorsView(AppBuilderBaseView):
    default_view = "vendors_index"
    route_base = "/vendors_management"

    def __init__(self, *args, **kwargs):
        self.folio_client = kwargs.get("folio_client")
        if self.folio_client is None:
            self.folio_client = FolioClient(
                Variable.get("okapi_url"),
                "sul",
                Variable.get("folio_user"),
                Variable.get("folio_password")
            )
        super().__init__(*args, **kwargs)

    def _get_vendors(self):
        """
        Returns vendors from FOLIO
        """
        cql_query = "(((code=YANKEE-SUL*) or (code=Harrassowitz*) or (code=CASALI-SUL*)))"
        vendor_result = requests.get(
            f"{self.folio_client.okapi_url}/organizations-storage/organizations?query={cql_query}",
            headers=self.folio_client.okapi_headers)
        vendor_result.raise_for_status()
        return vendor_result.json()

    @expose("/")
    def vendors_index(self):
        vendors = self._get_vendors()
        return self.render_template("vendors.html", vendors=vendors)