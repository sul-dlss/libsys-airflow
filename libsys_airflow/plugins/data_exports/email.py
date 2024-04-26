import ast
import logging

from typing import Union

from jinja2 import Template

from airflow.models import Variable
from airflow.utils.email import send_email

logger = logging.getLogger(__name__)

OCLC_CODES_LOOKUP = {
    "CASUM": "Lane",
    "HIN": "Hoover",
    "RCJ": "Law",
    "S7Z": "Business",
    "STF": "SUL",
}

OCLC_FAILED_TEMPLATE = Template(
    """<h2>Failed OCLC Record Exports for {{ library_name }}</h2>
    <ul>
    {% for uuid in instance_uuids %}
    <li>
        <a href="{{ folio_url }}/inventory/view/{{ uuid }}">Instance {{ uuid }}</a>
    </li>
    {% endfor %}
    </ul>

    """
)

OCLC_SUCCEED_TEMPLATE = Template(
    """<h2>{{ library_name}} Successful OCLC Exported Records</h2>
    <ul>
    {% for uuid in instance_uuids %}
    <li>
        <a href="{{ folio_url }}/inventory/view/{{ uuid }}">Instance {{ uuid }}</a>
    </li>
    {% endfor %}
    </ul>
    """
)


def _oclc_email_addresses(library_name: str) -> list:
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    sul_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_SUL")
    law_to_email_addr = Variable.get("ORAFIN_TO_EMAIL_LAW")

    match library_name:
        case "Law":
            email_addrs = [devs_to_email_addr, law_to_email_addr]

        case "SUL":
            email_addrs = [devs_to_email_addr, sul_to_email_addr]

        case _:
            email_addrs = [devs_to_email_addr]

    return email_addrs


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


def generate_multiple_oclc_identifiers_email(multiple_codes: Union[list, str]):
    """
    Generates an email for review by staff when multiple OCLC numbers
    exist for a record
    """
    if isinstance(multiple_codes, str):
        multiple_codes = ast.literal_eval(multiple_codes)
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

    html_content = _oclc_identifiers(multiple_codes, folio_url)  # type: ignore

    send_email(
        to=[
            sul_to_email_addr,
            law_to_email_addr,
            devs_to_email_addr,
        ],
        subject="Review Instances with Multiple OCLC Indentifiers",
        html_content=html_content,
    )


def generate_oclc_transmission_email(
    type_of: str, instance_uuids: list, oclc_code: str
):
    folio_url = Variable.get("FOLIO_URL")

    library_name = OCLC_CODES_LOOKUP[oclc_code]

    email_addrs = _oclc_email_addresses(library_name)

    match type_of:
        case "failure":
            subject = f"{library_name} Failed OCLC Exports"
            html_content = OCLC_FAILED_TEMPLATE.render(
                library_name=library_name,
                instance_uuids=instance_uuids,
                folio_url=folio_url,
            )

        case "success":
            subject = f"{library_name} Successful OCLC Exports"
            html_content = OCLC_SUCCEED_TEMPLATE.render(
                library_name=library_name,
                instance_uuids=instance_uuids,
                folio_url=folio_url,
            )

    send_email(to=email_addrs, subject=subject, html_content=html_content)
