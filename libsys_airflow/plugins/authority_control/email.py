import logging

from airflow.models import Variable

from libsys_airflow.plugins.shared.utils import (
    is_production,
    send_email_with_server_name,
    dag_run_url,
)

logger = logging.getLogger(__name__)


def email_report(**kwargs):
    """
    Emails result of folio-data-import to developers and SUL

    Args:
    - bash_result (str): result of the bash command
    """
    bash_result = kwargs.get("bash_result")
    devs_email = Variable.get("EMAIL_DEVS")
    to_emails = [devs_email]

    if is_production():
        sul_email = Variable.get("OCLC_EMAIL_SUL")
        to_emails.append(sul_email)

    url_dag_run = dag_run_url(**kwargs)
    body = (
        f"""<p>{bash_result}</p><p>DAG Run<a href="{url_dag_run}">{url_dag_run}</a>"""
    )
    send_email_with_server_name(
        to=to_emails,
        subject="Folio Data Import",
        html_content=body,
    )
    logger.info(f"Emailing load report: {bash_result}")


def email_deletes_report(**kwargs):
    """
    Emails Report of deleting FOLIO Authority Records
    """
    staff_email = kwargs.get("email")
    devs_email = Variable.get("EMAIL_DEVS")
    to_emails = [staff_email, devs_email]
    logger.info(f"To_emails: {to_emails}")

    url_dag_run = dag_run_url(**kwargs)
    body = _generate_delete_report(dag_run_url=url_dag_run, **kwargs)

    send_email_with_server_name(
        to=to_emails,
        subject="Delete Authority FOLIO Records Results",
        html_content=body,
    )
    logger.info(f"Sent email to {staff_email}")


def _generate_delete_report(**kwargs):
    """
    Generates HTML for Deleting FOLIO Authority Records
    """
    dag_run_url: str = kwargs.get("dag_run_url")
    deleted: int = kwargs.get("deleted", 0)
    errors: list = kwargs.get("errors", [])
    missing: list = kwargs.get("missing", [])
    multiples: list = kwargs.get("multiples", [])

    html = f"""<h2>FOLIO Authority Record Deletion</h2>
    <p>DAG Run <a href="{dag_run_url}">{dag_run_url}</a></p>
    <h3>Successfully deleted {deleted:,}</h3>"""
    html += f"<h3>Missing {len(missing):,} FOLIO Authority Records for 001s</h3>\n<ol>"
    for row in missing:
        html += f"<li>{row}</li>\n"
    html += f"</ol>\n<h3>{len(multiples):,} Multiple FOLIO Authority Records</h3><ol>"
    for row in multiples:
        html += f"<li>{row}</li>\n"
    html += f"</ol><h3>{len(errors):,} Errors</h3><ol>"
    for row in errors:
        html += f"<li>{row}</li>\n"
    html += "</ol>"

    return html
