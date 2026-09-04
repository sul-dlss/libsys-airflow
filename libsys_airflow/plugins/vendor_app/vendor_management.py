import logging
import os
import pathlib
import re
import shutil
from datetime import datetime, UTC
from urllib.parse import quote

from airflow.sdk import Variable
from airflow_client.client import DagRunApi, TriggerDAGRunPostBody
from airflow_client.client.models.dag_run_response import DAGRunResponse
from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from folioclient import FolioClient
from honeybadger.contrib.fastapi import HoneybadgerRoute
from starlette.datastructures import FormData

from libsys_airflow.plugins.shared.airflow_api_client import api_client
from libsys_airflow.plugins.shared.auth import require_view_access
from libsys_airflow.plugins.shared.csrf import (
    CSRFCookieMiddleware,
    csrf_field,
    csrf_protect,
    csrf_token,
)

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
from libsys_airflow.plugins.shared.utils import (
    folio_name,
    redirect_with_query_params,
)

logger = logging.getLogger(__name__)

URL_PREFIX = "/vendor_management"

app = FastAPI(
    route_class=HoneybadgerRoute,
    # "Dashboard" is the name in this plugin's external_views entry, which is what
    # Airflow itself passes when deciding whether to show the menu item.
    dependencies=[Depends(require_view_access("Dashboard"))],
)
app.add_middleware(CSRFCookieMiddleware)

templates = Jinja2Templates(
    directory=pathlib.Path(__file__).resolve().parent / "templates"
)
templates.env.filters["urlencode"] = lambda value: quote(str(value), safe="")
templates.env.globals["url_prefix"] = URL_PREFIX
templates.env.globals["csrf_field"] = csrf_field
templates.env.globals["csrf_token"] = csrf_token

app.mount(
    "/static",
    StaticFiles(directory=pathlib.Path(__file__).resolve().parent / "static"),
    name="static",
)


@app.middleware("http")
async def shutdown_session_middleware(request: Request, call_next):
    try:
        return await call_next(request)
    finally:
        Session.remove()


def _folio_client():
    return FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )


async def _form_data(request: Request) -> FormData:
    return await request.form()


def _redirect(url: str, message: str | None = None) -> RedirectResponse:
    if message:
        return redirect_with_query_params(url, status_code=302, message=message)
    return redirect_with_query_params(url, status_code=302)


@app.get("/")
def dashboard(request: Request):
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
        .all()
    )
    errors_files = (
        Session()
        .query(VendorFile)
        .filter(
            VendorFile.status.in_([FileStatus.fetching_error, FileStatus.loading_error])
        )
        .order_by(VendorFile.updated)
        .all()
    )

    return templates.TemplateResponse(
        request,
        "vendors/dashboard.html",
        {
            "in_progress_files": in_progress_files,
            "errors_files": errors_files,
            "folio_base_url": Variable.get("FOLIO_URL"),
            "folio_name": folio_name(),
            "message": request.query_params.get("message"),
        },
    )


@app.get("/vendors")
def vendors(request: Request, filter: str = "all"):
    if filter == "active_interfaces":
        vendor_list = Vendor.with_active_vendor_interfaces(Session())
    elif filter == "interfaces":
        vendor_list = Vendor.with_vendor_interfaces(Session())
    else:
        vendor_list = Session().query(Vendor).order_by(Vendor.display_name)
    return templates.TemplateResponse(
        request,
        "vendors/index.html",
        {"vendors": vendor_list, "filter": filter, "folio_name": folio_name()},
    )


@app.get("/vendors/{vendor_id}")
def vendor(vendor_id: int, request: Request):
    vendor = Session().query(Vendor).get(vendor_id)
    if vendor is None:
        raise HTTPException(status_code=404)
    """
    When upgrading to FOLIO Client > 1.0.0, consider using access_token instead of okapi_token.
    """
    client = _folio_client()
    return templates.TemplateResponse(
        request,
        "vendors/vendor.html",
        {
            "vendor": vendor,
            "folio_name": folio_name(),
            "okapi_url": client.okapi_url,
            "okapi_token": client.okapi_token,
            "message": request.query_params.get("message"),
        },
    )


@app.post("/vendors/{vendor_id}/interfaces", dependencies=[Depends(csrf_protect)])
def create_vendor_interface(vendor_id: int):
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
    return _redirect(f"{URL_PREFIX}/interfaces/{interface.id}/edit")


@app.post("/vendors/{vendor_id}/sync", dependencies=[Depends(csrf_protect)])
def vendor_sync(vendor_id: int):
    vendor = Session().query(Vendor).get(vendor_id)
    _trigger_folio_vendor_sync_dag(vendor)
    return _redirect(
        f"{URL_PREFIX}/vendors/{vendor.id}",
        message="Refresh of vendor data from FOLIO requested.",
    )


@app.get("/interfaces/{interface_id}")
def interface(interface_id: int, request: Request):
    interface = Session().query(VendorInterface).get(interface_id)
    if interface is None:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse(
        request,
        "vendors/interface.html",
        {
            "interface": interface,
            "folio_name": folio_name(),
            "message": request.query_params.get("message"),
        },
    )


@app.get("/interfaces/{interface_id}/edit")
def interface_edit_form(interface_id: int, request: Request):
    interface = Session().query(VendorInterface).get(interface_id)
    return templates.TemplateResponse(
        request,
        "vendors/interface-edit.html",
        {
            "interface": interface,
            "job_profiles": job_profiles(),
            "folio_name": folio_name(),
        },
    )


@app.post("/interfaces/{interface_id}/edit", dependencies=[Depends(csrf_protect)])
def interface_edit(
    interface_id: int, form: FormData = Depends(_form_data)  # noqa: B008
):
    session = Session()
    interface = session.query(VendorInterface).get(interface_id)

    _update_vendor_interface_form(interface, form)
    session.commit()
    return _redirect(f"{URL_PREFIX}/interfaces/{interface.id}")


def _update_vendor_interface_form(interface, form):
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

    if 'display-name' in form.keys():
        interface.display_name = form['display-name']

    if 'note' in form.keys() and len(form['note'].strip()) > 1:
        interface.note = form['note']

    if 'processing-delay-in-days' in form.keys():
        interface.processing_delay_in_days = int(form['processing-delay-in-days'] or 0)

    if 'remote-path' in form.keys():
        interface.remote_path = form['remote-path']

    if 'file-pattern' in form.keys():
        interface.file_pattern = form['file-pattern']

    if 'active' in form.keys():
        interface.active = form['active'] == 'true'

    if 'additional-email-recipients' in form.keys():
        interface.additional_email_recipients = form['additional-email-recipients']

    # form passes package-name as empty string (if not filled in)
    if 'package-name' in form.keys():
        processing_options = {}
        processing_options['package_name'] = form['package-name']
        processing_options['prepend_001'] = {
            "tag": "001",
            "data": form['prepend-001'],
        }
        processing_options['change_marc'] = []
        processing_options['delete_marc'] = []
        processing_options['add_subfield'] = []
        if form.get("archive-regex") is None:
            processing_options["archive_regex"] = ""
        else:
            processing_options["archive_regex"] = form["archive-regex"]

        for name, value in form.items():
            if name.startswith('remove-field'):
                processing_options['delete_marc'].append(value)
            if m := re.match(r'^add-subfield-tag-(\d+)', name):
                tag = form.get(f"add-subfield-tag-{m.group(1)}")
                eval_subfield = form.get(f"add-subfield-eval-{m.group(1)}")
                pattern = form.get(f"add-subfield-pattern-{m.group(1)}")
                subfield_code = form.get(f"add-subfield-code-{m.group(1)}")
                subfield_value = form.get(f"add-subfield-value-{m.group(1)}")
                if tag:
                    processing_options["add_subfield"].append(
                        {
                            "tag": tag,
                            "eval_subfield": eval_subfield,
                            "pattern": pattern,
                            "subfields": [
                                {"code": subfield_code, "value": subfield_value}
                            ],
                        }
                    )
            if m := re.match(r'^move-field-from-(\d+)', name):
                # use the identifier on the "from" form name to determine the
                # corresponding name for the "to" form name
                to_tag = form.get(f"move-field-to-{m.group(1)}")
                to_indicator1 = form.get(f"move-indicator1-to-{m.group(1)}")
                to_indicator2 = form.get(f"move-indicator2-to-{m.group(1)}")
                from_indicator1 = form.get(f"move-indicator1-from-{m.group(1)}")
                from_indicator2 = form.get(f"move-indicator2-from-{m.group(1)}")
                if to_tag:
                    processing_options['change_marc'].append(
                        {
                            "from": {
                                "tag": value,
                                "indicator1": from_indicator1,
                                "indicator2": from_indicator2,
                            },
                            "to": {
                                "tag": to_tag,
                                "indicator1": to_indicator1,
                                "indicator2": to_indicator2,
                            },
                        }
                    )

        interface.processing_options = processing_options

    return interface


_FILE_UPLOAD_FIELD = File(default=None, alias="file-upload")


@app.post("/interfaces/{interface_id}/file", dependencies=[Depends(csrf_protect)])
def file_upload(interface_id: int, file_upload: UploadFile | None = _FILE_UPLOAD_FIELD):
    if file_upload is None or not file_upload.filename:
        return _redirect(
            f"{URL_PREFIX}/interfaces/{interface_id}",
            message="No file uploaded. Make sure to click Browse... and select a file.",
        )
    _handle_file_upload(interface_id, file_upload)
    return _redirect(
        f"{URL_PREFIX}/interfaces/{interface_id}",
        message="File uploaded and queued for processing.",
    )


def _handle_file_upload(interface_id, file_upload):
    session = Session()
    interface = session.query(VendorInterface).get(interface_id)
    download_path = get_download_path(
        interface.vendor.folio_organization_uuid, interface.interface_uuid
    )

    filepath = _save_file(download_path, file_upload)

    vendor_file = _create_vendor_file(interface, file_upload, filepath, session)
    archive_file(download_path, vendor_file, session)
    _trigger_processing_dag(vendor_file, session)


def _save_file(path, file_upload):
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, file_upload.filename)
    with open(filepath, "wb") as out_file:
        shutil.copyfileobj(file_upload.file, out_file)
    return filepath


def _create_vendor_file(interface, file_upload, filepath, session):
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


@app.post("/interfaces/{interface_id}/fetch", dependencies=[Depends(csrf_protect)])
def interface_fetch(interface_id: int):
    session = Session()
    interface = session.query(VendorInterface).get(interface_id)
    _trigger_fetcher_dag(interface)

    return _redirect(
        f"{URL_PREFIX}/interfaces/{interface_id}",
        message=f"Requested fetch of {interface.display_name}",
    )


@app.post("/interfaces/{interface_id}/test", dependencies=[Depends(csrf_protect)])
def interface_test(interface_id: int):
    session = Session()
    interface = session.query(VendorInterface).get(interface_id)

    try:
        conn_id = create_connection(interface.folio_interface_uuid)
        create_hook(conn_id)
        message = "Test succeeded"
    except Exception as e:
        logger.error(f"Test failed for interface {interface.id}: {e}")
        message = "Test failed"

    return _redirect(f"{URL_PREFIX}/interfaces/{interface.id}", message=message)


@app.post("/interfaces/{interface_id}/delete", dependencies=[Depends(csrf_protect)])
def interface_delete(interface_id: int):
    session = Session()
    interface = session.query(VendorInterface).get(interface_id)
    vendor_id = interface.vendor_id
    session.delete(interface)
    session.commit()

    return _redirect(f"{URL_PREFIX}/vendors/{vendor_id}", message="Interface deleted")


@app.get("/files/{file_id}")
def file_detail(file_id: int, request: Request):
    session = Session()
    file = session.query(VendorFile).get(file_id)
    if file is None:
        raise HTTPException(status_code=404)
    return templates.TemplateResponse(
        request,
        "vendors/file.html",
        {
            "file": file,
            "FileStatus": FileStatus,
            "folio_name": folio_name(),
            "message": request.query_params.get("message"),
        },
    )


@app.post("/files/{file_id}", dependencies=[Depends(csrf_protect)])
def file_update(
    file_id: int, request: Request, form: FormData = Depends(_form_data)  # noqa: B008
):
    session = Session()
    file = session.query(VendorFile).get(file_id)

    message = None

    expected_processing_time = form.get('expected-processing-time')
    if expected_processing_time and isinstance(expected_processing_time, str):
        try:
            file.expected_processing_time = datetime.fromisoformat(
                expected_processing_time
            )
        except ValueError:
            message = f"invalid date: {expected_processing_time}"

    # The key "status" will not be in the form if it can no longer be manually set to loaded.
    # Also, to prevent a possible race codition ensure that the current status is allowed
    # to transition to loaded.
    if 'status' in form and file.status.can_set_loaded():
        file.status = form.get('status', file.status)
        now = datetime.utcnow()
        file.updated = now
        file.loaded_timestamp = now
        file.loaded_history = file.loaded_history + [now.isoformat()]

    session.commit()
    return templates.TemplateResponse(
        request,
        "vendors/file.html",
        {
            "file": file,
            "FileStatus": FileStatus,
            "folio_name": folio_name(),
            "message": message,
        },
    )


@app.post("/files/{file_id}/load", dependencies=[Depends(csrf_protect)])
def load_file(file_id: int, redirect_url: str | None = None):
    session = Session()
    file = session.query(VendorFile).get(file_id)
    _trigger_processing_dag(file, session)

    return _redirect(
        redirect_url or f"{URL_PREFIX}/",
        message=f"Requested reload of {file.vendor_filename}",
    )


@app.get("/files/{file_id}/download/{type}")
def download_file(type: str, file_id: int, request: Request):
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

    logger.info(f"Downloading {filename} from {path}")
    if not os.path.exists(os.path.join(path, filename)):
        referer = request.headers.get("referer") or f"{URL_PREFIX}/"
        return _redirect(referer, message=f"Oops, {filename} is not available.")

    return FileResponse(os.path.join(path, filename), filename=filename)


@app.post("/files/{file_id}/reset_fetch", dependencies=[Depends(csrf_protect)])
def reset_fetch(file_id: int):
    session = Session()
    file = session.query(VendorFile).get(file_id)
    file.status = FileStatus.not_fetched
    session.commit()

    return _redirect(
        f"{URL_PREFIX}/",
        message=f"Requested fetch of {file.vendor_filename} with next daily vendor download.",
    )


def _trigger_processing_dag(vendor_file, session):
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        trigger_dag_body = TriggerDAGRunPostBody(
            conf={
                "filename": vendor_file.vendor_filename,
                "vendor_uuid": vendor_file.vendor_interface.vendor.folio_organization_uuid,
                "vendor_interface_uuid": vendor_file.vendor_interface.interface_uuid,
                "dataload_profile_uuid": vendor_file.vendor_interface.folio_data_import_profile_uuid,
            }
        )
        api_response: DAGRunResponse = api_instance.trigger_dag_run(
            'default_data_processor', trigger_dag_body
        )

        dag_run_id = api_response.dag_run_id
        logger.info(
            f"Triggered DAG {api_response.dag_id} for {vendor_file.vendor_filename}"
        )
        vendor_file.dag_run_id = dag_run_id
        vendor_file.expected_processing_time = api_response.queued_at
        vendor_file.updated = datetime.now(UTC)
        vendor_file.status = FileStatus.loading
        session.commit()
        logger.info(
            f"Updated vendor_file {vendor_file}: dag_run_id={dag_run_id} queued date={api_response.queued_at.isoformat()}"
        )


def _trigger_fetcher_dag(interface):
    with api_client() as airflow_api_client:
        logger.info(f"Interface {interface.remote_path}")
        api_instance = DagRunApi(airflow_api_client)
        trigger_dag_body = TriggerDAGRunPostBody(
            conf={
                "vendor_interface_name": interface.display_name,
                "vendor_code": interface.vendor.vendor_code_from_folio,
                "vendor_uuid": interface.vendor.folio_organization_uuid,
                "vendor_interface_uuid": interface.folio_interface_uuid,
                "dataload_profile_uuid": interface.folio_data_import_profile_uuid,
                "remote_path": interface.remote_path or "",
                "filename_regex": interface.file_pattern,
            }
        )
        api_response: DAGRunResponse = api_instance.trigger_dag_run(
            "data_fetcher", trigger_dag_body
        )
        logger.info(f"Triggered DAG {api_response.dag_id} for {interface.display_name}")


def _trigger_folio_vendor_sync_dag(vendor):
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        trigger_dag_body = TriggerDAGRunPostBody(
            conf={
                "folio_org_uuid": vendor.folio_organization_uuid,
            },
        )
        api_response: DAGRunResponse = api_instance.trigger_dag_run(
            'folio_vendor_sync', trigger_dag_body
        )
        logger.info(f"Triggered DAG {api_response.dag_id} for {vendor.display_name}")
