from datetime import date
import logging
import pathlib

from airflow.configuration import conf
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy.orm import Session
from jinja2 import Template

# from libsys_airflow.plugins.vendor_app.database import Session
from libsys_airflow.plugins.vendor.models import VendorInterface
from libsys_airflow.plugins.vendor.marc import (
    is_marc,
    extract_double_zero_one_field_values,
)
from libsys_airflow.plugins.vendor.edi import invoice_count
from libsys_airflow.plugins.shared.utils import send_email_with_server_name


logger = logging.getLogger(__name__)


@task
def files_fetched_email_task(downloaded_files: list, kwargs: dict):
    if len(downloaded_files) < 1:
        logger.info("Skipping sending email since no files downloaded.")
        return
    kwargs["downloaded_files"] = downloaded_files
    kwargs["vendor_interface_url"] = _vendor_interface_url(
        kwargs["vendor_uuid"], kwargs["vendor_interface_uuid"]
    )
    kwargs["interface_additional_emails"] = _additional_email_recipients(
        kwargs["vendor_interface_uuid"]
    )

    send_files_fetched_email(**kwargs)


def send_files_fetched_email(**kwargs):
    kwargs["date"] = date.today().isoformat()
    subject = Template(
        "{{vendor_interface_name}} ({{vendor_code}}) - Daily Fetch Report ({{date}}) [{{environment}}]"
    ).render(kwargs)
    send_email_with_server_name(
        to=_email_recipients(kwargs.get('interface_additional_emails', None)),
        subject=subject,
        html_content=_files_fetched_html_content(**kwargs),
    )


def _files_fetched_html_content(**kwargs):
    template = Template(
        """
        <h5>{{vendor_interface_name}} ({{vendor_code}}) - <a href="{{vendor_interface_url}}">{{vendor_interface_uuid}}</a></h5>

        <p>
            Files fetched:
            <ul>
            {% for filename in downloaded_files %}
                <li>{{ filename }}</li>
            {% endfor %}
            </ul>
        </p>
        """
    )
    return template.render(kwargs)


def _email_recipients(recipients) -> str:
    recipients = recipients or []
    email_recipients = [Variable.get('VENDOR_LOADS_TO_EMAIL')]
    email_recipients.extend(recipients)

    return ','.join(email_recipients)


def _additional_email_recipients(vendor_interface_uuid) -> list:
    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        interface = VendorInterface.load(
            vendor_interface_uuid, session
        )

    recipients = interface.additional_email_recipients
    if recipients is None:
        return []

    return recipients.replace(',', ' ').split()


def _vendor_interface_url(vendor_uuid, vendor_interface_uuid):
    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        vendor_interface = VendorInterface.load_with_vendor(
            vendor_uuid, vendor_interface_uuid, session
        )
        airflow_url = conf.get('webserver', 'base_url')
        if not airflow_url.endswith("/"):
            airflow_url = f"{airflow_url}/"
        return f"{airflow_url}vendor_management/interfaces/{vendor_interface.id}"


@task
def file_loaded_email_task(**kwargs):
    processed_params = kwargs["processed_params"]
    params = kwargs["params"]
    job_execution_id = kwargs["job_execution_id"]
    job_summary = kwargs["job_summary"]
    kwargs = {**processed_params, **params, **job_summary}
    kwargs["job_execution_id"] = job_execution_id
    send_file_loaded_email(**kwargs)


def send_file_loaded_email(**kwargs):
    kwargs["job_execution_url"] = (
        f"{Variable.get('FOLIO_URL')}/data-import/job-summary/{kwargs['job_execution_id']}"
    )
    kwargs["file_path"] = pathlib.Path(kwargs["download_path"]) / kwargs['filename']

    if is_marc(kwargs["file_path"]):
        kwargs["double_zero_ones"] = extract_double_zero_one_field_values(
            kwargs["file_path"]
        )
        html_content = _file_loaded_bib_html_content(**kwargs)
    else:
        kwargs["records_count"] = invoice_count(kwargs["file_path"])
        html_content = _file_loaded_edi_html_content(**kwargs)

    kwargs["interface_additional_emails"] = _additional_email_recipients(
        kwargs["vendor_interface_uuid"]
    )
    subject = Template(
        "{{vendor_interface_name}} ({{vendor_code}}) - ({{filename}}) - File Load Report [{{environment}}]"
    ).render(kwargs)
    send_email_with_server_name(
        to=_email_recipients(kwargs.get('interface_additional_emails', None)),
        subject=subject,
        html_content=html_content,
    )


def _file_loaded_edi_html_content(**kwargs):

    kwargs["srs_created"] = kwargs["srs_stats"].get("totalCreatedEntities", 0)
    kwargs["instance_errors"] = kwargs["instance_stats"].get("totalErrors", 0)
    kwargs["vendor_interface_url"] = _vendor_interface_url(
        kwargs["vendor_uuid"], kwargs["vendor_interface_uuid"]
    )

    template = Template(
        """
        <h5>FOLIO Catalog EDI Load started on {{load_time}}</h5>
        <h6>{{vendor_interface_name}} ({{vendor_code}}) - <a href="{{vendor_interface_url}}">{{vendor_interface_uuid}}</a></h6>

        <p>Filename {{filename}} - {{job_execution_url}}</p>
        <p>{{records_count}} invoices read from EDI file.</p>
        <p>{{srs_created}} SRS records created</p>
        <p>{{instance_errors}} Instance errors</p>
        """
    )
    return template.render(kwargs)


def _file_loaded_bib_html_content(**kwargs):

    kwargs["instance_created"] = kwargs["instance_stats"].get("totalCreatedEntities", 0)
    kwargs["instance_updated"] = kwargs["instance_stats"].get("totalUpdatedEntities", 0)
    kwargs["instance_discarded"] = kwargs["instance_stats"].get(
        "totalDiscardedEntities", 0
    )
    kwargs["instance_errors"] = kwargs["instance_stats"].get("totalErrors", 0)

    kwargs["vendor_interface_url"] = _vendor_interface_url(
        kwargs["vendor_uuid"], kwargs["vendor_interface_uuid"]
    )

    kwargs["srs_created"] = kwargs["srs_stats"].get("totalCreatedEntities", 0)
    kwargs["srs_updated"] = kwargs["srs_stats"].get("totalUpdatedEntities", 0)
    kwargs["srs_discarded"] = kwargs["srs_stats"].get("totalDiscardedEntities", 0)
    kwargs["srs_errors"] = kwargs["srs_stats"].get("totalErrors", 0)

    template = Template(
        """
        <h5>FOLIO Catalog MARC Load started on {{load_time}}</h5>
        <h6>{{vendor_interface_name}} ({{vendor_code}}) - <a href="{{vendor_interface_url}}">{{vendor_interface_uuid}}</a></h6>

        <p>Filename {{filename}} - {{job_execution_url}}</p>
        <p>{{records_count}} bib record(s) read from MARC file.</p>
        <p>{{srs_created}} SRS records created</p>
        <p>{{srs_updated}} SRS records updated</p>
        <p>{{srs_discarded}} SRS records discarded</p>
        <p>{{srs_errors}} SRS errors</p>
        <p>{{instance_created}} Instance records created</p>
        <p>{{instance_updated}} Instance records updated</p>
        <p>{{instance_discarded}} Instance records discarded</p>
        <p>{{instance_errors}} Instance errors</p>

        <h5>001 Values</h5>
        {% if double_zero_ones | length > 0 -%}
          <ul>
          {% for double_zero_one in double_zero_ones -%}
            <li>{{ double_zero_one }}</li>
          {% endfor -%}
          </ul>
        {%- else -%}
          <p>No 001 fields</p>
        {%- endif -%}
        """
    )
    return template.render(**kwargs)


@task
def file_not_loaded_email_task(**kwargs):
    processed_params = kwargs["processed_params"]
    params = kwargs["params"]
    kwargs = {**processed_params, **params}
    kwargs["vendor_interface_url"] = _vendor_interface_url(
        kwargs["vendor_uuid"], kwargs["vendor_interface_uuid"]
    )
    send_file_not_loaded_email(**kwargs)


def send_file_not_loaded_email(**kwargs):
    kwargs["interface_additional_emails"] = _additional_email_recipients(
        kwargs["vendor_interface_uuid"]
    )
    send_email_with_server_name(
        to=_email_recipients(kwargs.get('interface_additional_emails', None)),
        subject=Template(
            "{{vendor_interface_name}} ({{vendor_code}}) - ({{filename}}) - File Processed [{{environment}}]"
        ).render(kwargs),
        html_content=_file_not_loaded_html_content(**kwargs),
    )


def _file_not_loaded_html_content(**kwargs):
    return Template(
        """
        <h5>{{vendor_interface_name}} ({{vendor_code}}) - <a href="{{vendor_interface_url}}">{{vendor_interface_uuid}}</a></h5>

        <p>
            File processed, but not loaded: {{filename}}
        </p>
        """
    ).render(kwargs)
