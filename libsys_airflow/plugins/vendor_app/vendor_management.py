import re
import logging
import os
from datetime import datetime

from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import Variable
from flask_appbuilder import expose, BaseView
from flask import request, redirect, url_for, flash, send_from_directory

from libsys_airflow.plugins.vendor.job_profiles import (
    job_profiles,
    get_job_profile_name,
)
from libsys_airflow.plugins.vendor.models import (
    Vendor,
    VendorInterface,
    VendorFile,
    FileStatus,
)
from libsys_airflow.plugins.vendor.paths import download_path as get_download_path
from libsys_airflow.plugins.vendor.paths import archive_path as get_archive_path
from libsys_airflow.plugins.vendor_app.database import Session
from libsys_airflow.plugins.vendor.archive import archive_file
from libsys_airflow.plugins.airflow.connections import create_connection
from libsys_airflow.plugins.vendor.download import create_hook

logger = logging.getLogger(__name__)


class VendorManagementView(BaseView):
    default_view = "dashboard"
    route_base = "/vendor_management"

    @expose("/")
    def dashboard(self):
        in_progress_files = (
            Session()
            .query(VendorFile)
            .filter(
                VendorFile.status.in_(
                    [
                        FileStatus.not_fetched,
                        FileStatus.fetched,
                        FileStatus.loading,
                    ]
                )
            )
            .order_by(VendorFile.updated)
        )
        errors_files = (
            Session()
            .query(VendorFile)
            .filter(
                VendorFile.status.in_(
                    [FileStatus.fetching_error, FileStatus.loading_error]
                )
            )
            .order_by(VendorFile.updated)
        )

        return self.render_template(
            "vendors/dashboard.html",
            in_progress_files=in_progress_files,
            errors_files=errors_files,
            folio_base_url=Variable.get("FOLIO_URL"),
        )

    @expose("/vendors")
    def vendors(self):
        filter = request.args.get("filter", default="all")
        if filter == "active_interfaces":
            vendors = Vendor.with_active_vendor_interfaces(Session())
        elif filter == "interfaces":
            vendors = Vendor.with_vendor_interfaces(Session())
        else:
            vendors = Session().query(Vendor).order_by(Vendor.display_name)
        return self.render_template(
            "vendors/index.html", vendors=vendors, filter=filter
        )

    @expose("/vendors/<int:vendor_id>")
    def vendor(self, vendor_id):
        vendor = Session().query(Vendor).get(vendor_id)
        return self.render_template("vendors/vendor.html", vendor=vendor)

    @expose("/vendors/<int:vendor_id>/interfaces", methods=["POST"])
    def create_vendor_interface(self, vendor_id):
        session = Session()
        vendor = session.query(Vendor).get(vendor_id)
        interface = VendorInterface(
            vendor_id=vendor.id,
            display_name=f"{vendor.display_name} - Upload Only",
            active=True,
            assigned_in_folio=False,
        )
        session.add(interface)
        session.commit()
        return redirect(
            url_for('VendorManagementView.interface_edit', interface_id=interface.id)
        )

    @expose("/vendors/<int:vendor_id>/sync", methods=["POST"])
    def vendor_sync(self, vendor_id):
        vendor = Session().query(Vendor).get(vendor_id)
        self._trigger_folio_vendor_sync_dag(vendor)
        flash("Refresh of vendor data from FOLIO requested.")
        return redirect(url_for('VendorManagementView.vendor', vendor_id=vendor.id))

    @expose("/interfaces/<int:interface_id>")
    def interface(self, interface_id):
        interface = Session().query(VendorInterface).get(interface_id)
        return self.render_template("vendors/interface.html", interface=interface)

    @expose("/interfaces/<int:interface_id>/edit", methods=['GET', 'POST'])
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

        if 'folio-data-import-profile-uuid' in form.keys():
            if form['folio-data-import-profile-uuid'] == '':
                interface.folio_data_import_profile_uuid = None
                interface.folio_data_import_processing_name = None
            else:
                interface.folio_data_import_profile_uuid = form[
                    'folio-data-import-profile-uuid'
                ]
                interface.folio_data_import_processing_name = get_job_profile_name(
                    form['folio-data-import-profile-uuid']
                )

        if 'processing-delay-in-days' in form.keys():
            interface.processing_delay_in_days = int(
                form['processing-delay-in-days'] or 0
            )

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
            if form['archive-regex'] != '':
                processing_options['archive_regex'] = form['archive-regex']

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

    @expose("/interfaces/<int:interface_id>/file", methods=["POST"])
    def file_upload(self, interface_id):
        file_upload = request.files["file-upload"]
        if not file_upload or file_upload.filename == "":
            flash("No file uploaded. Make sure to click Browse... and select a file.")
        else:
            self._handle_file_upload(interface_id, file_upload)
            flash("File uploaded and queued for processing.")
        return redirect(
            url_for("VendorManagementView.interface", interface_id=interface_id)
        )

    def _handle_file_upload(self, interface_id, file_upload):
        session = Session()
        interface = session.query(VendorInterface).get(interface_id)
        download_path = get_download_path(
            interface.vendor.folio_organization_uuid, interface.interface_uuid
        )

        filepath = self._save_file(download_path, file_upload)

        vendor_file = self._create_vendor_file(
            interface, file_upload, filepath, session
        )
        archive_file(download_path, vendor_file, session)
        self._trigger_processing_dag(vendor_file, session)

    def _save_file(self, path, file_upload):
        os.makedirs(path, exist_ok=True)
        filepath = os.path.join(path, file_upload.filename)
        file_upload.save(filepath)
        return filepath

    def _create_vendor_file(self, interface, file_upload, filepath, session):
        existing_vendor_file = VendorFile.load_with_vendor_interface(
            interface, file_upload.filename, session
        )
        if existing_vendor_file:
            session.delete(existing_vendor_file)
        new_vendor_file = VendorFile(
            created=datetime.utcnow(),
            updated=datetime.utcnow(),
            vendor_interface_id=interface.id,
            vendor_filename=file_upload.filename,
            filesize=os.path.getsize(filepath),
            status=FileStatus.uploaded,
        )
        session.add(new_vendor_file)
        session.commit()
        return new_vendor_file

    @expose("/interfaces/<int:interface_id>/fetch", methods=["POST"])
    def interface_fetch(self, interface_id):
        session = Session()
        interface = session.query(VendorInterface).get(interface_id)
        self._trigger_fetcher_dag(interface)

        flash(f"Requested fetch of {interface.display_name}")

        return redirect(
            url_for("VendorManagementView.interface", interface_id=interface_id)
        )

    @expose("/interfaces/<int:interface_id>/test", methods=['POST'])
    def interface_test(self, interface_id):
        session = Session()
        interface = session.query(VendorInterface).get(interface_id)

        try:
            conn_id = create_connection(interface.folio_interface_uuid)
            create_hook(conn_id)
            flash("Test succeeded")
        except Exception as e:
            flash(f"Test failed: {e}")

        return redirect(
            url_for('VendorManagementView.interface', interface_id=interface.id)
        )

    @expose("/interfaces/<int:interface_id>/delete", methods=["POST"])
    def interface_delete(self, interface_id):
        session = Session()
        interface = session.query(VendorInterface).get(interface_id)
        vendor_id = interface.vendor_id
        session.delete(interface)
        session.commit()

        flash("Interface deleted")
        return redirect(url_for('VendorManagementView.vendor', vendor_id=vendor_id))

    @expose("/files/<int:file_id>", methods=["GET", "POST"])
    def file(self, file_id):
        session = Session()
        file = session.query(VendorFile).get(file_id)
        if request.method == 'POST' and 'expected-load-time' in request.form:
            try:
                expected_processing_time = request.form['expected-load-time']
                if expected_processing_time != '':
                    file.expected_processing_time = datetime.fromisoformat(
                        expected_processing_time
                    )
                else:
                    file.expected_processing_time = None
                session.commit()
            except ValueError:
                flash("invalid date: {request.form['expected-load-time']}")

        return self.render_template("vendors/file.html", file=file)

    @expose("/files/<int:file_id>/load", methods=["POST"])
    def load_file(self, file_id):
        session = Session()
        file = session.query(VendorFile).get(file_id)
        self._trigger_processing_dag(file, session)
        flash(f"Requested reload of {file.vendor_filename}")
        redirect_url = request.args.get("redirect_url")

        return redirect(redirect_url)

    @expose("/files/<int:file_id>/download/<type>", methods=["GET"])
    def download_file(self, file_id, type):
        session = Session()
        file = session.query(VendorFile).get(file_id)
        if type == 'processed':
            path = get_download_path(
                file.vendor_interface.vendor.folio_organization_uuid,
                file.vendor_interface.interface_uuid,
            )
            filename = file.processed_filename
        else:
            path = get_archive_path(
                file.vendor_interface.vendor.folio_organization_uuid,
                file.vendor_interface.interface_uuid,
                file.archive_date,
            )
            filename = file.vendor_filename

        print(f"Downloading {filename} from {path}")
        if not os.path.exists(os.path.join(path, filename)):
            flash(f"Oops, {filename} is not available.")
            return redirect(request.referrer)

        return send_from_directory(
            path,
            filename,
            as_attachment=True,
        )

    @expose("/files/<int:file_id>/reset_fetch", methods=["POST"])
    def reset_fetch(self, file_id):
        session = Session()
        file = session.query(VendorFile).get(file_id)
        file.status = FileStatus.not_fetched
        session.commit()
        flash(
            f"Requested fetch of {file.vendor_filename} with next daily vendor download."
        )

        return redirect(url_for("VendorManagementView.dashboard"))

    def _trigger_processing_dag(self, vendor_file, session):
        dag_run = trigger_dag(
            'default_data_processor',
            conf={
                "filename": vendor_file.vendor_filename,
                "vendor_uuid": vendor_file.vendor_interface.vendor.folio_organization_uuid,
                "vendor_interface_uuid": vendor_file.vendor_interface.interface_uuid,
                "dataload_profile_uuid": vendor_file.vendor_interface.folio_data_import_profile_uuid,
            },
        )

        logger.info(f"Triggered DAG {dag_run} for {vendor_file.vendor_filename}")
        vendor_file.dag_run_id = dag_run.run_id
        vendor_file.expected_processing_time = dag_run.execution_date
        vendor_file.updated = datetime.utcnow()
        vendor_file.status = FileStatus.loading
        session.commit()
        logger.info(
            f"Updated vendor_file {vendor_file}: dag_run_id={dag_run.run_id} execution_date={dag_run.execution_date}"
        )

    def _trigger_fetcher_dag(self, interface):
        dag = trigger_dag(
            'data_fetcher',
            conf={
                "vendor_interface_name": interface.display_name,
                "vendor_code": interface.vendor.vendor_code_from_folio,
                "vendor_uuid": interface.vendor.folio_organization_uuid,
                "vendor_interface_uuid": interface.folio_interface_uuid,
                "dataload_profile_uuid": interface.folio_data_import_profile_uuid,
                "remote_path": interface.remote_path,
                "filename_regex": interface.file_pattern,
            },
        )
        logger.info(f"Triggered DAG {dag} for {interface.display_name}")

    def _trigger_folio_vendor_sync_dag(self, vendor):
        dag = trigger_dag(
            'folio_vendor_sync',
            conf={
                "folio_org_uuid": vendor.folio_organization_uuid,
            },
        )
        logger.info(f"Triggered DAG {dag} for {vendor.display_name}")
