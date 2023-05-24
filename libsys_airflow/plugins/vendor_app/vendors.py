import re
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from flask import request, redirect, url_for
from flask_appbuilder import expose, BaseView
from sqlalchemy.orm import Session

from libsys_airflow.plugins.vendor.models import Vendor, VendorInterface
from libsys_airflow.plugins.vendor.job_profiles import job_profiles


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

    @expose("/interface/<int:interface_id>/edit", methods=['GET', 'POST'])
    def interface_edit(self, interface_id):
        session = self._get_session()
        interface = session.query(VendorInterface).get(interface_id)

        if request.method == 'GET':
            return self.render_template(
                "vendors/interface-edit.html",
                interface=interface,
                job_profiles=job_profiles(),
            )
        else:
            self._update_vendor_interface_form(interface, request.form)
            session.commit()
            return redirect(
                url_for('VendorManagementView.interface', interface_id=interface.id)
            )

    def _get_session(self):
        pg_hook = PostgresHook("vendor_loads")
        return Session(pg_hook.get_sqlalchemy_engine())

    def _update_vendor_interface_form(self, interface, form):
        """
        Save the supplied vendor interface data to the database and return the
        VendorInterface object.
        """

        if 'folio-data-import-profile-uuid' in form.keys():
            interface.folio_data_import_profile_uuid = form[
                'folio-data-import-profile-uuid'
            ]

        if 'folio-data-import-processing-name' in form.keys():
            interface.folio_data_import_processing_name = form[
                'folio-data-import-processing-name'
            ]

        if 'processing-delay-in-days' in form.keys():
            interface.processing_delay_in_days = int(form['processing-delay-in-days'])

        if 'remote-path' in form.keys():
            interface.remote_path = form['remote-path']

        if 'file-pattern' in form.keys():
            interface.file_pattern = form['file-pattern']

        if 'active' in form.keys():
            interface.active = form['active'] == 'true'

        if 'package-name' in form.keys():
            processing_options = {}
            processing_options['package_name'] = form['package-name']
            processing_options['change_marc'] = []
            processing_options['delete_marc'] = []

            for name, value in form.items():
                if name.startswith('remove-field'):
                    processing_options['delete_marc'].append(value)
                if m := re.match(r'^move-field-from-(\d+)', name):
                    # use the identifier on the "from" form name to determine the
                    # corresponding name for the "to" form name
                    to_name = f"move-field-to-{m.group(1)}"
                    to_value = form.get(to_name)
                    if to_value:
                        processing_options['change_marc'].append(
                            {"from": value, "to": to_value}
                        )

            interface.processing_options = processing_options

        return interface
