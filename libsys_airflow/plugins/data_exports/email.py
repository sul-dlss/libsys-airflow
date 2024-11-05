import logging
import pathlib
import urllib

from jinja2 import Template

from airflow.configuration import conf
from airflow.decorators import task
from airflow.models import Variable
from libsys_airflow.plugins.shared.utils import send_email_with_server_name

from libsys_airflow.plugins.shared.utils import is_production

logger = logging.getLogger(__name__)


def _cohort_emails():
    return {
        "business": Variable.get("OCLC_EMAIL_BUS"),
        "hoover": Variable.get("OCLC_EMAIL_HOOVER"),
        "lane": Variable.get("OCLC_EMAIL_LANE"),
        "law": Variable.get("OCLC_EMAIL_LAW"),
        "sul": Variable.get("OCLC_EMAIL_SUL"),
    }


def _match_oclc_library(**kwargs):
    library: str = kwargs["library"]
    to_emails: list = kwargs["to_emails"]
    subject_line: str = kwargs["subject_line"]
    cohort_emails: dict = kwargs["cohort_emails"]

    match library:
        case "CASUM":
            to_emails.insert(0, cohort_emails.get("lane"))
            subject_line += " Lane"

        case "HIN":
            to_emails.insert(0, cohort_emails.get("hoover"))
            subject_line += " Hoover"

        case "RCJ":
            to_emails.insert(0, cohort_emails.get("law"))
            subject_line += " Law"

        case "S7Z":
            to_emails.insert(0, cohort_emails.get("business"))
            subject_line += " Business"

        case "STF":
            to_emails.insert(0, cohort_emails.get("sul"))
            subject_line += " SUL"

    return to_emails, subject_line


def _oclc_report_html(report: str, library: str):

    report_path = pathlib.Path(report)
    report_type = report_path.parent.name
    airflow_url = conf.get('webserver', 'base_url')  # type: ignore

    if not airflow_url.endswith("/"):
        airflow_url = f"{airflow_url}/"

    report_url = f"{airflow_url}data_export_oclc_reports/{library}/{report_type}/{report_path.name}"

    return f"""{report_type} link: <a href="{report_url}">{report_path.name}</a>"""


def dag_run_url(dag_run) -> str:
    airflow_url = conf.get('webserver', 'base_url')
    if not airflow_url.endswith("/"):
        airflow_url = f"{airflow_url}/"
    params = urllib.parse.urlencode({"dag_run_id": dag_run.run_id})
    return f"{airflow_url}dags/{dag_run.id}/grid?{params}"


def generate_holdings_errors_emails(error_reports: dict):
    """
    Generates emails for holdings set errors for cohort libraries
    """
    devs_email = Variable.get("EMAIL_DEVS")
    cohort_emails = _cohort_emails()

    for library, report in error_reports.items():
        to_emails = [
            devs_email,
        ]
        report_path = pathlib.Path(report)
        report_type = report_path.parent.name

        match report_type:

            case "set_holdings_match":
                subject_line = "OCLC: Set holdings match error for"

            case "unset_holdings":
                subject_line = "OCLC: Unset holdings error for"

            case _:
                subject_line = "OCLC: Set holdings error for"

        to_emails, subject_line = _match_oclc_library(
            library=library,
            to_emails=to_emails,
            cohort_emails=cohort_emails,
            subject_line=subject_line,
        )

        if not is_production():
            to_emails.pop(0)  # Should only send report to libsys devs

        html_content = _oclc_report_html(report, library)

        send_email_with_server_name(
            to=to_emails, subject=subject_line, html_content=html_content
        )


def generate_oclc_new_marc_errors_email(error_reports: dict):
    """
    Generates emails for each library for OCLC MARC errors for new-to-OCLC
    records
    """
    devs_email = Variable.get("EMAIL_DEVS")

    cohort_emails = _cohort_emails()

    airflow_url = conf.get('webserver', 'base_url')  # type: ignore

    if not airflow_url.endswith("/"):
        airflow_url = f"{airflow_url}/"

    subject_line = "OCLC: MARC Errors for New Record"

    for library, report in error_reports.items():
        to_emails = [
            devs_email,
        ]

        _match_oclc_library(
            library=library,
            to_emails=to_emails,
            cohort_emails=cohort_emails,
            subject_line=subject_line,
        )

        if not is_production():
            to_emails.pop(0)  # Should only send report to libsys devs

        html_content = _oclc_report_html(report, library)

        send_email_with_server_name(
            to=to_emails, subject=subject_line, html_content=html_content
        )


def generate_multiple_oclc_identifiers_email(**kwargs):
    """
    Generates an email for review by staff when multiple OCLC numbers
    exist for a record
    """
    reports = kwargs["reports"]

    if len(reports) < 1:
        logger.info("No multiple OCLC Identifiers")
        return

    folio_url = Variable.get("FOLIO_URL")
    devs_email = Variable.get("EMAIL_DEVS")
    cohort_emails = _cohort_emails()

    for library, report in reports.items():
        to_emails = [
            devs_email,
        ]

<<<<<<< HEAD
    if is_production():
        send_email_with_server_name(
            to=[
                devs_email,
                cohort_emails["business"],
                cohort_emails["hoover"],
                cohort_emails["lane"],
                cohort_emails["law"],
                cohort_emails["sul"],
            ],
            subject="Review Instances with Multiple OCLC Indentifiers",
            html_content=html_content,
        )
    else:
        folio_url = folio_url.replace("https://", "").replace(".stanford.edu", "")
        send_email_with_server_name(
            to=[
                devs_email,
            ],
            subject=f"{folio_url} - Review Instances with Multiple OCLC Indentifiers",
=======
        subject_line = "Review Instances with Multiple OCLC Identifiers"

        to_emails, subject_line = _match_oclc_library(
            library=library,
            to_emails=to_emails,
            cohort_emails=cohort_emails,
            subject_line=subject_line,
        )

        if is_production():
            subject_line = f"Production {subject_line}"
        else:
            folio_url = folio_url.replace("https://", "").replace(".stanford.edu", "")
            subject_line = f"{folio_url} {subject_line}"
            to_emails.pop(0)

        html_content = _oclc_report_html(report, library)

        send_email(
            to=to_emails,
            subject=subject_line,
>>>>>>> e0feae39 (bug: Use existing tasks to generate report for multiple OCLC numbers)
            html_content=html_content,
        )


def _failed_transmission_email_body(
    files: list, vendor: str, dag_id: str, dag_run_id: str, dag_run_url: str
):
    template = Template(
        """
        {% if vendor|length > 0 %}
        <h2>Failed to Transmit Files for {{ dag_id }} {{ vendor }}</h2>
        {% else %}
        <h2>Failed to Transmit Files for {{ dag_id }}</h2>
        {% endif %}
        <p><a href="{{ dag_run_url }}">{{ dag_run_id }}</a>
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
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        dag_run_url=dag_run_url,
    )


@task
def failed_transmission_email(files: list, **kwargs):
    """
    Generates an email listing files that failed to transmit
    Sends to libsys devs to troubleshoot
    """
    dag_run = kwargs["dag_run"]
    dag_id = dag_run.id
    dag_run_id = dag_run.run_id
    run_url = dag_run_url(dag_run)
    params = kwargs.get("params", {})
    full_dump_vendor = params.get("vendor", {})
    if len(files) == 0:
        logger.info("No failed files to send in email")
        return
    logger.info("Generating email of failed to transmit files")
    devs_to_email_addr = Variable.get("EMAIL_DEVS")

    html_content = _failed_transmission_email_body(
        files,
        full_dump_vendor,
        dag_id,
        dag_run_id,
        run_url,
    )

    send_email_with_server_name(
        to=[
            devs_to_email_addr,
        ],
        subject=f"Failed File Transmission for {dag_id} {dag_run_id}",
        html_content=html_content,
    )
