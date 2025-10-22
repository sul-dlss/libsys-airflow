import logging

from jinja2 import Template

from airflow.sdk import task, Variable

from libsys_airflow.plugins.shared.utils import (
    is_production,
    send_email_with_server_name,
)

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
    if len(dag_runs) < 1:
        return ""
    return Template(
        """
        <h2>Results from adding 979 fields Workflows</h2>
        <ol>
        {% for dag_run_id, result in dag_runs.items() %}
        <li>DAG Run <a href="{{ result.url }}">{{ dag_run_id }}</a> {{ result.state }}<br>
           Instances:
           <ul>
           {% for instance in result.instances %}
           <li><a href="{{ folio_url }}/inventory/view/{{ instance.uuid }}">{{ instance.uuid }}</a>
             Funds:
             <table>
               <tr>
                 <th>Name (if available)</th>
                 <th>Title</th>
               </tr>
               {% for fund in instance.funds %}
                <tr>
                  <td>{% if fund.name %}{{ fund.name }}{% endif %}</td>
                  <td>{{ fund.title }}</td>
                </tr>
               {% endfor %}
             </table>
           </li>
           {% endfor %}
           </ul>
        </li>
        {% endfor %}
        </ol>
    """
    ).render(dag_runs=dag_runs, folio_url=folio_url)


def _to_addresses():
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    bookplates_email_addr = Variable.get("BOOKPLATES_EMAIL")
    addresses = [devs_to_email_addr]
    if is_production():
        addresses.append(bookplates_email_addr)
    return addresses


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

    html_content = _new_updated_bookplates_email_body(
        new=new_bookplates,
        updated=updated_bookplates,
    )

    send_email_with_server_name(
        to=_to_addresses(),
        subject="Digital bookplates new and updated metadata",
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

    html_content = _deleted_from_argo_email_body(deleted_druids)

    send_email_with_server_name(
        to=_to_addresses(),
        subject="Deleted Druids from Argo for Digital bookplates",
        html_content=html_content,
    )


@task
def missing_fields_email(**kwargs):
    failures = kwargs["failures"]

    if len(failures) < 1:
        logger.info("No missing fields")
        return True

    html_content = _missing_fields_body(failures)

    send_email_with_server_name(
        to=_to_addresses(),
        subject="Missing Fields for Digital Bookplates",
        html_content=html_content,
    )

    return True


@task
def summary_add_979_dag_runs(**kwargs):
    dag_runs = kwargs["dag_runs"]
    additional_email = kwargs.get("email")
    folio_url = Variable.get("FOLIO_URL")

    to_emails = _to_addresses()
    if additional_email:
        to_emails.append(additional_email)

    html_content = _summary_add_979_email(dag_runs, folio_url)

    if len(html_content.strip()) > 0:
        send_email_with_server_name(
            to=to_emails,
            subject="Summary of Adding 979 fields to MARC Workflows",
            html_content=html_content,
        )
