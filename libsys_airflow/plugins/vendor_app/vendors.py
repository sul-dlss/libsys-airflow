import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from flask_appbuilder import expose, BaseView
from sqlalchemy.orm import Session

from libsys_airflow.plugins.vendor.models import Vendor


logger = logging.getLogger(__name__)


class VendorManagementView(BaseView):
    default_view = "index"
    route_base = "/vendors"

    @expose("/")
    def index(self):
        session = self._get_session()
        vendors = session.query(Vendor).order_by(Vendor.display_name)
        return self.render_template("vendors/index.html", vendors=vendors)

    @expose("/<int:vendor_id>")
    def vendor(self, vendor_id):
        session = self._get_session()
        vendor = session.query(Vendor).get(vendor_id)
        return self.render_template("vendors/vendor.html", vendor=vendor)

    def _get_session(self):
        pg_hook = PostgresHook("vendor_loads")
        return Session(pg_hook.get_sqlalchemy_engine())
