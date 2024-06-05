import logging

from jinja2 import Template

from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.email import send_email

from libsys_airflow.plugins.data_exports.transmission_tasks import (
    is_production,
)

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
    devs_email = Variable.get("EMAIL_DEVS")
    bus_email = Variable.get("OCLC_EMAIL_BUS")
    hoover_email = Variable.get("OCLC_EMAIL_HOOVER")
    lane_email = Variable.get("OCLC_EMAIL_LANE")
    law_email = Variable.get("OCLC_EMAIL_LAW")
    sul_email = Variable.get("OCLC_EMAIL_SUL")

    html_content = _oclc_identifiers(multiple_codes, folio_url)

    if is_production():
        send_email(
            to=[
                devs_email,
                bus_email,
                hoover_email,
                lane_email,
                law_email,
                sul_email,
            ],
            subject="Review Instances with Multiple OCLC Indentifiers",
            html_content=html_content,
        )
    else:
        folio_url = folio_url.replace("https://", "").replace(".stanford.edu", "")
        send_email(
            to=[
                devs_email,
            ],
            subject=f"{folio_url} - Review Instances with Multiple OCLC Indentifiers",
            html_content=html_content,
        )


def _failed_transmission_email_body(
    files: list, vendor: str, base_url: str, dag_id: str, dag_run_id: str
):
    template = Template(
        """
        {% if vendor|length > 0 %}
        <h2>Failed to Transmit Files for {{ dag_id }} {{ vendor }}</h2>
        {% else %}
        <h2>Failed to Transmit Files for {{ dag_id }}</h2>
        {% endif %}
        <p><a href="https://{{ base_url }}/dags/{{ dag_id }}/grid?dag_run_id={{ dag_run_id }}">{{ dag_run_id }}</a>
        <p>These files failed to transmit</p>
        <ol>
        {% for row in files %}
        <li>
          {{ row }}
        </li>
        {% endfor %}
        </ol>
    """
    )

    return template.render(
        files=files,
        vendor=vendor,
        base_url=base_url,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
    )


@task
def failed_transmission_email(files: list, **kwargs):
    """
    Generates an email listing files that failed to transmit
    Sends to libsys devs to troubleshoot
    """
    dag = kwargs["dag_run"]
    dag_id = dag.id
    dag_run_id = dag.run_id
    params = kwargs.get("params", {})
    full_dump_vendor = params.get("vendor", {})
    if len(files) == 0:
        logger.info("No failed files to send in email")
        return
    logger.info("Generating email of failed to transmit files")
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    base_url = Variable.get("AIRFLOW__WEBSERVER__BASE_URL")

    html_content = _failed_transmission_email_body(
        files, full_dump_vendor, base_url, dag_id, dag_run_id
    )

    send_email(
        to=[
            devs_to_email_addr,
        ],
        subject=f"Failed File Transmission for {dag_id} {dag_run_id}",
        html_content=html_content,
    )
