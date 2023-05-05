import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from sqlalchemy.orm import Session

from libsys_airflow.plugins.vendor.models import Vendor


logger = logging.getLogger(__name__)


class VendorManagementView(AppBuilderBaseView):
    default_view = "vendors_index"
    route_base = "/vendors"

    def _get_vendors(self):
        """
        Retrieves vendors from vendor_loads database
        """
        pg_hook = PostgresHook("vendor_loads")
        with Session(pg_hook.get_sqlalchemy_engine()) as session:
            return session.query(Vendor).order_by(Vendor.display_name)

    @expose("/")
    def vendors_index(self):
        vendors = self._get_vendors()
        return self.render_template("vendors/index.html", vendors=vendors)
