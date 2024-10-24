import logging

from jinja2 import Template

from airflow.configuration import conf
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.email import send_email

from libsys_airflow.plugins.shared.utils import is_production

logger = logging.getLogger(__name__)


def _deleted_from_argo_email_body(deleted_druids: list) -> str:
    return Template(
        """
      <h2>Deleted from Argo -- Digital Bookplates Druids</h2>
      <ul>
      {% for row in druids %}
        <li>Title: {{ row.title }}, Fund name: {{ row.fund_name }}, Druid: {{ row.druid }}</li>
      {% endfor %}
      </ul>
      """
    ).render(druids=deleted_druids)


def _missing_fields_body(failures: list) -> str:
    return Template(
        """
      <h2>Missing Fields from Argo Digital Bookplates Druids</h2>
      <ul>
      {% for failure in failures %}
      <li>Failure: {{ failure.failure }},{% if failure.title %} Title: {{ failure.title }},{% endif %}{% if failure.fund_name %} Fund name: {{ failure.fund }},{% endif %} Druid: {{ failure.druid }}</li>
      {% endfor %}
      </ul>
      """
    ).render(failures=failures)


def _new_updated_bookplates_email_body(new: list, updated: list):
    template = Template(
        """
        <h2>New digital bookplates metadata</h2>
        {% if new|length > 0 %}
        <table>
          <tr>
            <th>Fund Name</th>
            <th>Druid</th>
            <th>Filename</th>
            <th>Title</th>
          </tr>
        {% for row in new %}
          <tr>
            <td>{{row["fund_name"]}}</td>
            <td>{{row["druid"]}}</td>
            <td>{{row["image_filename"]}}</td>
            <td>{{row["title"]}}</td>
          </tr>
        {% endfor %}
        </table>
        {% else %}
        <p>No new digital bookplates this run.</p>
        {% endif %}
        <h2>Updated digital bookplates metadata</h2>
        {% if updated|length > 0 %}
        <table>
          <tr>
            <th>Fund Name</th>
            <th>Druid</th>
            <th>Filename</th>
            <th>Title</th>
            <th>Reason</th>
          </tr>
        {% for row in updated %}
          <tr>
            <td>{{row["fund_name"]}}</td>
            <td>{{row["druid"]}}</td>
            <td>{{row["image_filename"]}}</td>
            <td>{{row["title"]}}</td>
            <td>{{row["reason"]}}</td>
          </tr>
        {% endfor %}
        </table>
        {% else %}
        <p>No updated digital bookplates metadata this run.</p>
        {% endif %}
    """
    )

    return template.render(
        new=new,
        updated=updated,
    )


def _summary_add_979_email(dag_runs: list, folio_url: str) -> str:
    airflow_url = conf.get('webserver', 'base_url')  # type: ignore

    if not airflow_url.endswith("/"):
        airflow_url = f"{airflow_url}/"
    dag_url = f"{airflow_url}dags/digital_bookplate_979/grid?run_id="
    return Template(
        """
        <h2>Results from adding 979 fields Workflows</h2>
        <ol>
        {% for dag_run_id, result in dag_runs.items() %}
        <li>DAG Run <a href="{{ dag_url }}{{ dag_run_id|urlencode }}">{{ dag_run_id }}</a> {{ result.state }}<br>
           Instances:
           <ul>
           {% for uuid in result.instance_uuids %}
           <li><a href="{{ folio_url }}/inventory/view/{{ uuid }}">{{ uuid }}</a></li>
           {% endfor %}
           </ul>
        </li>
        {% endfor %}
        </ol>
    """
    ).render(dag_runs=dag_runs, folio_url=folio_url, dag_url=dag_url)


@task
def bookplates_metadata_email(**kwargs):
    """
    Generates an email listing new and updated digital bookplates metadata
    """
    new_bookplates = kwargs.get("new", [])
    updated_bookplates = kwargs.get("updated", [])

    if new_bookplates is None:
        new_bookplates = []

    if updated_bookplates is None:
        updated_bookplates = []

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


@task
def deleted_from_argo_email(**kwargs):
    """
    Sends email of druids deleted from Argo but are still in the Digital
    Bookplates database.
    """
    deleted_druids = kwargs["deleted_druids"]
    if len(deleted_druids) < 1:
        logger.info("No Deleted Druids from Argo")
        return

    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    bookplates_email_addr = Variable.get("BOOKPLATES_EMAIL")
    folio_url = Variable.get("FOLIO_URL")

    html_content = _deleted_from_argo_email_body(deleted_druids)

    if is_production():
        send_email(
            to=[
                bookplates_email_addr,
                devs_to_email_addr,
            ],
            subject="Deleted Druids from Argo for Digital bookplates",
            html_content=html_content,
        )
    else:
        folio_url = folio_url.replace("https://", "").replace(".stanford.edu", "")
        send_email(
            to=[
                devs_to_email_addr,
            ],
            subject=f"{folio_url} - Deleted Druids from Argo for Digital bookplate",
            html_content=html_content,
        )


@task
def missing_fields_email(**kwargs):
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    bookplates_email_addr = Variable.get("BOOKPLATES_EMAIL")
    folio_url = Variable.get("FOLIO_URL")

    failures = kwargs["failures"]

    if len(failures) < 1:
        logger.info("No missing fields")
        return True

    html_content = _missing_fields_body(failures)

    if is_production():
        send_email(
            to=[
                bookplates_email_addr,
                devs_to_email_addr,
            ],
            subject="Missing Fields for Digital Bookplates",
            html_content=html_content,
        )
    else:
        folio_env = folio_url.replace("https://", "").replace(".stanford.edu", "")
        send_email(
            to=[devs_to_email_addr],
            subject=f"{folio_env} - Missing Fields for Digital Bookplates",
            html_content=html_content,
        )

    return True


@task
def summary_add_979_dag_runs(**kwargs):
    dag_runs = kwargs["dag_runs"]
    additional_email = kwargs.get("email")

    folio_url = Variable.get("FOLIO_URL")
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    bookplates_email_addr = Variable.get("BOOKPLATES_EMAIL")

    to_emails = [devs_to_email_addr]
    if additional_email:
        to_emails.append(additional_email)
    html_content = _summary_add_979_email(dag_runs, folio_url)

    if is_production():
        to_emails.append(bookplates_email_addr)
        send_email(
            to=to_emails,
            subject="Summary of Adding 979 fields to MARC Workflows",
            html_content=html_content,
        )
    else:
        folio_env = folio_url.replace("https://", "").replace(".stanford.edu", "")
        send_email(
            to=to_emails,
            subject=f"{folio_env} - Summary of Adding 979 fields to MARC",
            html_content=html_content,
        )
