import os
from datetime import date, datetime
import logging

from airflow.configuration import conf
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy.orm import Session
from jinja2 import Template

from libsys_airflow.plugins.vendor.models import VendorInterface

logger = logging.getLogger(__name__)


def _email_enabled() -> bool:
    return conf.has_option("smtp", "smtp_host")


def email_args() -> dict:
    if not _email_enabled():
        return {}

    return {
        "email_on_failure": True,
        "email": os.getenv('VENDOR_LOADS_TO_EMAIL'),
    }


@task
def files_fetched_email_task(
    vendor_name: str,
    vendor_code: str,
    vendor_interface_uuid: str,
    downloaded_files: list[str],
):
    if not downloaded_files:
        logger.info("Skipping sending email since no files downloaded.")
        return

    if not _email_enabled():
        logger.info("Email not enabled.")
        return

    send_files_fetched_email(
        vendor_name, vendor_code, vendor_interface_uuid, downloaded_files
    )


def send_files_fetched_email(
    vendor_name, vendor_code, vendor_interface_uuid, downloaded_files
):
    if _email_enabled():
        send_email(
            os.getenv('VENDOR_LOADS_TO_EMAIL'),
            f"{vendor_name} ({vendor_code}) - {vendor_interface_uuid} - Daily Fetch Report ({date.today().isoformat()})",
            _files_fetched_html_content(
                vendor_name, vendor_code, vendor_interface_uuid, downloaded_files
            ),
        )


def _files_fetched_html_content(
    vendor_name, vendor_code, vendor_interface_uuid, downloaded_files
):
    template = Template(
        """
        <h5>{{vendor_name}} ({{vendor_code}}) - <a href="{{vendor_interface_url}}">{{vendor_interface_uuid}}</a></h5>

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
    return template.render(
        vendor_name=vendor_name,
        vendor_code=vendor_code,
        vendor_interface_uuid=vendor_interface_uuid,
        downloaded_files=downloaded_files,
        vendor_interface_url=_vendor_interface_url(vendor_interface_uuid),
    )


def _vendor_interface_url(vendor_interface_uuid):
    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        vendor_interface = VendorInterface.load(vendor_interface_uuid, session)
        return f"{conf.get('webserver', 'base_url')}/vendor_management/interfaces/{vendor_interface.id}"


@task
def file_loaded_email_task(
    vendor_code: str,
    vendor_name: str,
    folio_job_execution_uuid: str,
    filename: str,
    load_time: datetime,
    records_count: int,
    srs_stats: dict[str, int],
    instance_stats: dict[str, int],
):
    if not _email_enabled():
        logger.info("Email not enabled.")
        return

    send_file_loaded_email(
        vendor_code,
        vendor_name,
        folio_job_execution_uuid,
        filename,
        load_time,
        records_count,
        srs_stats,
        instance_stats,
    )


def send_file_loaded_email(
    vendor_code,
    vendor_name,
    folio_job_execution_uuid,
    filename,
    load_time,
    records_count,
    srs_stats,
    instance_stats,
):
    send_email(
        os.getenv('VENDOR_LOADS_TO_EMAIL'),
        f"{vendor_name} ({vendor_code}) - ({filename}) - File Load Report",
        _file_loaded_html_content(
            folio_job_execution_uuid,
            filename,
            load_time,
            records_count,
            srs_stats,
            instance_stats,
        ),
    )


def _file_loaded_html_content(
    folio_job_execution_uuid,
    filename,
    load_time,
    records_count,
    srs_stats,
    instance_stats,
):
    folio_base_url = Variable.get("FOLIO_URL")
    template = Template(
        """
        <h5>FOLIO Catalog MARC Load started on {{load_time}}</h5>

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
        """
    )
    return template.render(
        load_time=load_time,
        filename=filename,
        job_execution_url=f"{folio_base_url}/data-import/job-summary/{folio_job_execution_uuid}",
        records_count=records_count,
        srs_created=srs_stats["totalCreatedEntities"],
        srs_updated=srs_stats["totalUpdatedEntities"],
        srs_discarded=srs_stats["totalDiscardedEntities"],
        srs_errors=srs_stats["totalErrors"],
        instance_created=instance_stats["totalCreatedEntities"],
        instance_updated=instance_stats["totalUpdatedEntities"],
        instance_discarded=instance_stats["totalDiscardedEntities"],
        instance_errors=instance_stats["totalErrors"],
    )
