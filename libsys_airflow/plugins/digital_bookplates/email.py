import logging

from jinja2 import Template

from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.email import send_email

from libsys_airflow.plugins.shared.utils import is_production

logger = logging.getLogger(__name__)


def _new_updated_bookplates_email_body(new: list, updated: list):
    template = Template(
        """
        <h2>New digital bookplates metadata</h2>
        {% if new|length > 0 %}
        <ul>
        {% for row in new %}
        <li>
          {{ row }}
        </li>
        {% endfor %}
        </ul>
        {% else %}
        <p>No new digital bookplates this run.</p>
        {% endif %}
        <h2>Updated digital bookplates metadata</h2>
        {% if updated|length > 0 %}
        <ul>
        {% for row in updated %}
        <li>
          {{ row }}
        </li>
        {% endfor %}
        </ul>
        {% else %}
        <p>No updated digital bookplates metadata this run.</p>
        {% endif %}
    """
    )

    return template.render(
        new=new,
        updated=updated,
    )


@task
def bookplates_metadata_email(**kwargs):
    """
    Generates an email listing new and updated digital bookplates metadata
    """
    new_bookplates = kwargs.get("new", [])
    updated_bookplates = kwargs.get("updated", [])
    if len(new_bookplates) == 0:
        logger.info("No new bookplate metadata to send in email")
    else:
        logger.info("New bookplate metadata to send in email")

    if len(updated_bookplates) == 0:
        logger.info("No updated bookplate metadata to send in email")
    else:
        logger.info("Updated bookplate metadata to send in email")

    logger.info("Generating email of fetch digital bookplate metadata run")
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    bookplates_email_addr = Variable.get("BOOKPLATES_EMAIL")
    folio_url = Variable.get("FOLIO_URL")

    html_content = _new_updated_bookplates_email_body(
        new=new_bookplates,
        updated=updated_bookplates,
    )

    if is_production():
        send_email(
            to=[
                bookplates_email_addr,
                devs_to_email_addr,
            ],
            subject="Digital bookplates new and updated metadata",
            html_content=html_content,
        )
    else:
        folio_url = folio_url.replace("https://", "").replace(".stanford.edu", "")
        send_email(
            to=[
                devs_to_email_addr,
            ],
            subject=f"{folio_url} - Digital bookplates new and updated metadata",
            html_content=html_content,
        )
