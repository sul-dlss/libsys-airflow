import logging

from jinja2 import Template

from airflow.models import Variable
from airflow.utils.email import send_email

logger = logging.getLogger(__name__)


def _oclc_identifiers(multiple_codes: list, folio_url: str):
    template = Template(
        """<h2>Multiple OCLC Identifiers</h2>
    <p>These Instances contain multiple OCLC identifiers and need
    manual remediation to be uploaded to OCLC</p>
    <ul>
    {% for row in multiple_codes %}
     <li>
       <a href="{{folio_url}}/inventory/viewsource/{{row[0]}}">MARC view of Instance {{row[0]}}</a>
       with OCLC Identifiers {% for code in row[2] %}{{ code }}{% if not loop.list %}, {% endif %}{% endfor %}.
     </li>
    {% endfor %}
    </ul>
    """
    )

    return template.render(folio_url=folio_url, multiple_codes=multiple_codes)


def generate_multiple_oclc_identifiers_email(multiple_codes: list):
    """
    Generates an email for review by staff when multiple OCLC numbers
    exist for a record
    """
    if len(multiple_codes) < 1:
        logger.info("No multiple OCLC Identifiers")
        return
    logger.info(
        f"Generating Email of Multiple OCLC Identifiers for {len(multiple_codes)}"
    )
    folio_url = Variable.get("FOLIO_URL")
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")

    html_content = _oclc_identifiers(multiple_codes, folio_url)

    send_email(
        to=[
            sul_to_email_addr,
            law_to_email_addr,
            devs_to_email_addr,
        ],
        subject="Review Instances with Multiple OCLC Indentifiers",
        html_content=html_content,
    )
