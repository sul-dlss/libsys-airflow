import logging

from airflow.models import Variable

from libsys_airflow.plugins.shared.utils import send_email_with_server_name

from libsys_airflow.plugins.shared.utils import is_production, dag_run_url

logger = logging.getLogger(__name__)


def email_report(bash_result: str):
    """
    Emails result of folio-data-import to developers and SUL

    Args:
    - bash_result (str): result of the bash command
    """

    devs_email = Variable.get("EMAIL_DEVS")
    to_emails = [devs_email]

    if is_production:
        sul_email = Variable.get("OCLC_EMAIL_SUL")
        to_emails.append(sul_email)

    body = f"""<p>{bash_result}</p><p>DAG Run<a href="{{ dag_run_url }}">{dag_run_url}</a>"""
    send_email_with_server_name(
        to=to_emails,
        subject="Folio Data Import",
        html_content=body,
    )
    logger.info(f"Emailing load report: {bash_result}")
