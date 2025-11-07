import logging

from airflow.sdk import Variable

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
