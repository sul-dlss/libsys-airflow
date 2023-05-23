import re
import logging

from airflow.api.common.trigger_dag import trigger_dag
from flask_appbuilder import expose, BaseView
from flask import request, redirect, url_for, flash

from libsys_airflow.plugins.vendor.job_profiles import job_profiles
from libsys_airflow.plugins.vendor.models import (
    Vendor,
    VendorInterface,
    VendorFile,
    FileStatus,
)
from libsys_airflow.plugins.vendor_app.database import Session

logger = logging.getLogger(__name__)


class VendorManagementView(BaseView):
    default_view = "index"
    route_base = "/vendors"

    @expose("/")
    def index(self):
        vendors = Session().query(Vendor).order_by(Vendor.display_name)
        return self.render_template("vendors/index.html", vendors=vendors)

    @expose("/<int:vendor_id>")
    def vendor(self, vendor_id):
        vendor = Session().query(Vendor).get(vendor_id)
        return self.render_template("vendors/vendor.html", vendor=vendor)

    @expose("/interface/<int:interface_id>")
    def interface(self, interface_id):
        interface = Session().query(VendorInterface).get(interface_id)
        return self.render_template("vendors/interface.html", interface=interface)

    @expose("/interface/<int:interface_id>/edit", methods=['GET', 'POST'])
    def interface_edit(self, interface_id):
        session = Session()
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

    def _update_vendor_interface_form(self, interface, form):
        """
        Save the supplied vendor interface data to the database and return the
        VendorInterface object.
        """

        if form['folio-data-import-profile-uuid']:
            interface.folio_data_import_profile_uuid = form[
                'folio-data-import-profile-uuid'
            ]

        if form['folio-data-import-processing-name']:
            interface.folio_data_import_processing_name = form[
                'folio-data-import-processing-name'
            ]

        if form['processing-delay-in-days']:
            interface.processing_delay_in_days = int(form['processing-delay-in-days'])

        if form['remote-path']:
            interface.remote_path = form['remote-path']

        if form['file-pattern']:
            interface.file_pattern = form['file-pattern']

        if form['package-name']:
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

    @expose("/file/<int:file_id>/load", methods=["POST"])
    def load_file(self, file_id):
        session = Session()
        file = session.query(VendorFile).get(file_id)

        file.status = FileStatus.loading
        session.commit()
        dag = trigger_dag(
            'default_data_processor',
            conf={
                "filename": file.vendor_filename,
                "vendor_uuid": file.vendor_interface.vendor.folio_organization_uuid,
                "vendor_interface_uuid": file.vendor_interface.folio_interface_uuid,
                "dataload_profile_uuid": file.vendor_interface.folio_data_import_profile_uuid,
            },
        )
        logger.info(f"Triggered DAG {dag} for {file.vendor_filename}")
        flash(f"Requested reload of {file.vendor_filename}")

        return redirect(
            url_for(
                "VendorManagementView.interface", interface_id=file.vendor_interface_id
            )
        )
