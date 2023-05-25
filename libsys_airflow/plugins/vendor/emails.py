import os
from datetime import date
import logging

from airflow.configuration import conf
from airflow.decorators import task
from airflow.utils.email import send_email
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy.orm import Session
from jinja2 import Template

from libsys_airflow.plugins.vendor.models import VendorInterface

logger = logging.getLogger(__name__)


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
            _html_content(
                vendor_name, vendor_code, vendor_interface_uuid, downloaded_files
            ),
        )


def _email_enabled() -> bool:
    return conf.has_option("smtp", "smtp_host")


def _html_content(vendor_name, vendor_code, vendor_interface_uuid, downloaded_files):
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
