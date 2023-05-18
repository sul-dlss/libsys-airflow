import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from flask_appbuilder import expose, BaseView
from sqlalchemy.orm import Session

from libsys_airflow.plugins.vendor.models import Vendor, VendorInterface


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

    @expose("/interface/<int:interface_id>")
    def interface(self, interface_id):
        session = self._get_session()
        interface = session.query(VendorInterface).get(interface_id)
        return self.render_template("vendors/interface.html", interface=interface)

    def _get_session(self):
        pg_hook = PostgresHook("vendor_loads")
        return Session(pg_hook.get_sqlalchemy_engine())
