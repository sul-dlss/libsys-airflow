import logging
import pathlib

from jinja2 import Template

from airflow.configuration import conf
from airflow.decorators import task
from airflow.models import Variable
from libsys_airflow.plugins.shared.utils import send_email_with_server_name

from libsys_airflow.plugins.shared.utils import is_production, dag_run_url

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

    subject_template = "OCLC: MARC Errors for New Record"

    for library, report in error_reports.items():
        to_emails = [
            devs_email,
        ]

        to_emails, subject_line = _match_oclc_library(
            library=library,
            to_emails=to_emails,
            cohort_emails=cohort_emails,
            subject_line=subject_template,
        )

        if not is_production():
            to_emails.pop(0)  # Should only send report to libsys devs

        html_content = _oclc_report_html(report, library)

        send_email_with_server_name(
            to=to_emails, subject=subject_line, html_content=html_content
        )


@task
def generate_multiple_oclc_identifiers_email(**kwargs):
    """
    Generates an email for review by staff when multiple OCLC numbers
    exist for a record
    """
    reports = kwargs["reports"]

    if len(reports) < 1:
        logger.info("No multiple OCLC Identifiers")
        return

    cohort_emails = _cohort_emails()

    for library, report in reports.items():

        to_emails, subject_line = _match_oclc_library(
            library=library,
            to_emails=[Variable.get("EMAIL_DEVS")],
            cohort_emails=cohort_emails,
            subject_line="Review Instances with Multiple OCLC Identifiers",
        )

        if not is_production():
            to_emails.pop(0)  # Should only send report to libsys devs

        send_email_with_server_name(
            to=to_emails,
            subject=subject_line,
            html_content=_oclc_report_html(report, library),
        )


def _no_files_email_body(dag_id: str, dag_run_id: str, dag_run_url: str):
    template = Template(
        """
        <h2>No Files Found to Transmit for {{ dag_id }}</h2>
        <p><a href="{{ dag_run_url }}">{{ dag_run_id }}</a>
    """
    )

    return template.render(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        dag_run_url=dag_run_url,
    )


@task
def no_files_email(**kwargs):
    """
    Generates email for failing to find vendor files to export
    """
    dag_run = kwargs["dag_run"]
    dag_run_id = dag_run.run_id
    dag_id = dag_run.dag.dag_id

    run_url = dag_run_url(dag_run=dag_run)
    logger.info("Generating email of no files found to transmit")
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    html_content = _no_files_email_body(
        dag_id,
        dag_run_id,
        run_url,
    )

    send_email_with_server_name(
        to=[
            devs_to_email_addr,
        ],
        subject=f"No Files Found to Transmit for {dag_id}",
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
    dag_run_id = dag_run.run_id
    dag_id = dag_run.dag.dag_id

    run_url = dag_run_url(dag_run=dag_run)
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


def _missing_holdings_for_instances(report: str) -> str:
    report_path = pathlib.Path(report)
    airflow_url = conf.get('webserver', 'base_url')  # type: ignore

    if not airflow_url.endswith("/"):
        airflow_url = f"{airflow_url}/"

    report_url = (
        f"{airflow_url}data_export_oclc_reports/missing_holdings/{report_path.name}"
    )

    template = Template(
        """
        <h2>Instances with No Holdings Report</h2>
        <a href="{{ report_url }}">{{ report_name }}</a>
        """
    )
    return template.render(report_url=report_url, report_name=report_path.name)


def _upload_confirmation_email_body(
    vendor: str, record_id_kind: str, number_of_ids: int, uploaded_filename: str
) -> str:
    template = Template(
        """
        <h2>Data Export Upload Confirmation</h2>
        <p>Your file {{ uploaded_filename }} was successfully submitted for export to {{ vendor }} as {{ record_id_kind }}.</p>
        <p>Number of IDs submitted: {{ number_of_ids }}</p>
        <p>The records will be processed during the next scheduled data export.</p>
    """
    )

    return template.render(
        vendor=vendor,
        record_id_kind=record_id_kind,
        number_of_ids=number_of_ids,
        uploaded_filename=uploaded_filename,
    )


def _missing_marc_for_instances(**kwargs) -> str:
    instance_uuids: list = kwargs["instance_uuids"]
    dag_run = kwargs["dag_run"]
    dag_run_id = dag_run.run_id
    dag_id = dag_run.dag.dag_id

    folio_url: str = kwargs["folio_url"]

    run_url = dag_run_url(dag_run=dag_run)

    template = Template(
        """
        <h2>Instances with No MARC Records</h2>
        <h3>DAG: {{ dag_id }} Run: <a href="{{ run_url }}">{{ dag_run_id }}</a></h3>
        <ul>
        {% for uuid in instance_uuids %}
        <li><a href="{{ folio_url }}/inventory/view/{{ uuid }}">{{ uuid }}</a></li>
        {% endfor %}
        </ul>
        """
    )

    return template.render(
        dag_id=dag_id,
        run_url=run_url,
        dag_run_id=dag_run_id,
        instance_uuids=instance_uuids,
        folio_url=folio_url,
    )


def send_confirmation_email(**kwargs):
    vendor = kwargs["vendor"]
    user_email = kwargs["user_email"]
    record_id_kind = kwargs["record_id_kind"]
    number_of_ids = kwargs["number_of_ids"]
    uploaded_filename = kwargs["uploaded_filename"]

    if uploaded_filename is not None:
        logger.info("Generating upload confirmation email")
        email_addresses = [Variable.get("EMAIL_DEVS")]
        if user_email is not None:
            email_addresses.append(user_email)

        send_email_with_server_name(
            to=','.join(email_addresses),
            subject="Upload Confirmation for Data Export",
            html_content=_upload_confirmation_email_body(
                vendor,
                record_id_kind,
                number_of_ids,
                uploaded_filename,
            ),
        )

    return None


@task
def generate_no_holdings_instances_email(**kwargs):
    report = kwargs["report"]

    if report is None or len(report) < 1:
        logger.info("All instances have holdings records")
        return

    logger.info("Generating email for instances without holdings")
    email_addresses = [Variable.get("EMAIL_DEVS")]

    if is_production():
        email_addresses.append(Variable.get("OCLC_EMAIL_SUL"))

    send_email_with_server_name(
        to=email_addresses,
        subject="Instances without Holdings",
        html_content=_missing_holdings_for_instances(report),
    )


@task
def generate_missing_marc_email(**kwargs):
    instance_uuids = kwargs["missing_marc_instances"]
    is_oclc = kwargs.get("is_oclc", False)

    if len(instance_uuids) < 1:
        logger.info("No missing MARC records")
        return

    dag_run = kwargs["dag_run"]
    folio_url = Variable.get("FOLIO_URL")

    email_addresses = [Variable.get("EMAIL_DEVS")]

    if is_oclc and is_production():
        email_addresses.append(Variable.get("OCLC_EMAIL_SUL"))

    body = _missing_marc_for_instances(
        instance_uuids=instance_uuids,
        dag_run=dag_run,
        folio_url=folio_url,
    )

    send_email_with_server_name(
        to=email_addresses,
        subject=f"Instances missing MARC Records for {dag_run.dag.dag_id}",
        html_content=body,
    )
