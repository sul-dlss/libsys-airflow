import logging
import requests

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from flask_appbuilder import expose, ModelView, BaseView as AppBuilderBaseView
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask import flash, request, redirect, Response
from folioclient import FolioClient

from sqlalchemy import select
from sqlalchemy.orm import Session
from plugins.data_loading_management.models import Organization, Interface

logger = logging.getLogger(__name__)

class InterfaceView(ModelView):
    datamodel=SQLAInterface(Interface)

class OrganizationView(AppBuilderBaseView):
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
        pg_hook = PostgresHook("dataloading_app")
        vendors_stmt = select(Organization).order_by(Organization.name)
        vendors = []
        with Session(pg_hook.get_sqlalchemy_engine()) as session:
            for vendor in session.execute(vendors_stmt).scalars():
                vendors.append( {"id": vendor.id,
                                 "uuid": vendor.uuid,
                                 "name": vendor.name,
                                 "last_updated": vendor.last_folio_update })
        return vendors


    @expose("/")
    def vendors_index(self):
        vendors = self._get_vendors()
        return self.render_template("vendors.html", vendors=vendors)


    @expose("/retrieve")
    def vendor_retrival(self):
        """
        Retrieves specific vendors from FOLIO and adds/updates to Database
        """
        cql_query = "(((code=YANKEE-SUL*) or (code=Harrassowitz*) or (code=CASALI-SUL*)))"
        vendor_result = requests.get(
            f"{self.folio_client.okapi_url}/organizations-storage/organizations?query={cql_query}",
            headers=self.folio_client.okapi_headers)
        vendor_result.raise_for_status()
        
        update_vendors = []
        for vendor in vendor_result.json().get('organizations', []):
            update_vendors.append(
                Organization(name=vendor['name'],
                                uuid=vendor['id'],
                                last_folio_update=vendor['metadata']['updatedDate'])
            )
        pg_hook = PostgresHook("dataloading_app")
        with Session(pg_hook.get_sqlalchemy_engine()) as session:
            session.add_all(update_vendors)
            session.commit()
        return f"Added/Updated {len(update_vendors)}"

    

