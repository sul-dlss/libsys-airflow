import logging

from datetime import datetime
from pathlib import Path

from airflow.decorators import task
from airflow.models import Variable
from jinja2 import Environment, DictLoader

logger = logging.getLogger(__name__)

holdings_set_template = """
<h1>OCLC Holdings {% if match %}Matched{% endif %} Set Errors on {{ date }} for {{ library }}</h1>
<p>
  <a href="{{ dag_run.url }}">DAG Run</a>
</p>
<h2>FOLIO Instances that failed trying to set Holdings</h2>
<table>
  <thead>
    <tr>
      <th>Instance</th>
      <th>OCLC Response</th>
    </tr>
  </thead>
  <tbody>
{% for instance in instances.values() %}
  <tr>
    <td>
      <a href="{{ instance.folio_url }}">{{ instance.uuid }}</a>
    </td>
    <td>
    {% if instance.oclc_error %}
    {% include 'oclc-payload-template.html' %}
    {% else %}
     No response from OCLC set API call
     {% endif %}
    </td>
  </tr>
{% endfor %}
  </tbody>
</table>
"""

multiple_oclc_numbers_template = """
 <h1>Multiple OCLC Numbers on {{ date }} for {{ library }}</h1>

 <p>
  <a href="{{ dag_run.url }}">DAG Run</a>
 </p>

 <h2>FOLIO Instances with Multiple OCLC Numbers</h2>
 <ol>
{% for instance in instances.values() %}
 <li>
   <a href="{{ instance.folio_url }}">{{ instance.uuid }}</a>:
   <ul>
   {% for num in instance.oclc_numbers %}
    <li>{{ num }}</li>
   {% endfor %}
   </ul>
 </li>
{% endfor %}
  </ol>
"""


oclc_payload_template = """<ul>
        <li><strong>Control Number:</strong> {{ instance.oclc_error.controlNumber }}</li>
        <li><strong>Requested Control Number:</strong> {{ instance.oclc_error.requestedControlNumber }}</li>
        <li><strong>Institution:</strong>
           <ul>
             <li><em>Code:</em> {{ instance.oclc_error.institutionCode }}</li>
             <li><em>Symbol:</em> {{ instance.oclc_error.institutionSymbol }}</li>
           </ul>
        </li>
        <li><strong>First Time Use:</strong> {{ instance.oclc_error.firstTimeUse }}</li>
        <li><strong>Success:</strong> {{ instance.oclc_error.success }}</li>
        <li><strong>Message:</strong> {{ instance.oclc_error.message }}</li>
        <li><strong>Action:</strong> {{ instance.oclc_error.action }}</li>
     </ul>
"""

jinja_env = Environment(
    loader=DictLoader(
        {
            "holdings-set.html": holdings_set_template,
            "multiple-oclc-numbers.html": multiple_oclc_numbers_template,
            "oclc-payload-template.html": oclc_payload_template,
        }
    )
)


def _generate_holdings_set_report(**kwargs) -> dict:
    airflow_dir: str = kwargs.get('airflow', '/opt/airflow')
    airflow = Path(airflow_dir)
    dag_run: dict = kwargs['dag_run']
    folio_base_url: str = kwargs['folio_url']
    date: datetime = kwargs.get('date', datetime.utcnow())

    reports: dict = {}
    report_template = jinja_env.get_template("multiple-oclc-numbers.html")

    library_instances: dict = {}

    return _save_reports(
        name="set_holdings",
        reports=reports,
        date=date
    )


def _generate_multiple_oclc_numbers_report(**kwargs) -> dict:

    dag_run: dict = kwargs['dag_run']
    multiple_codes: list = kwargs['all_multiple_codes']
    folio_base_url: str = kwargs['folio_url']
    date: datetime = kwargs.get('date', datetime.utcnow())

    reports: dict = {}
    report_template = jinja_env.get_template("multiple-oclc-numbers.html")

    library_instances: dict = {}

    for row in multiple_codes:
        instance_uuid = row[0]
        library_code = row[1]
        oclc_codes = row[2]

        if library_code in library_instances:
            library_instances[library_code][instance_uuid] = {
                "oclc_numbers": oclc_codes
            }
        else:
            library_instances[library_code] = {
                instance_uuid: {"oclc_numbers": oclc_codes}
            }

    for library, instances in library_instances.items():
        for uuid, info in instances.items():
            info['folio_url'] = _folio_url(folio_base_url, uuid)
            info['uuid'] = uuid
        reports[library] = report_template.render(
            dag_run=dag_run,
            date=date.strftime("%d %B %Y"),
            library=library,
            instances=instances,
        )

    return _save_reports(
        name="multiple_oclc_numbers",
        reports=reports,
        date=date,
    )


def _folio_url(folio_base_url: str, instance_uuid: dict):
    return f"{folio_base_url}/inventory/view/{instance_uuid}"


def _save_reports(**kwargs) -> dict:
    name: str = kwargs['name']
    libraries_reports: dict = kwargs['reports']
    airflow_dir: str = kwargs.get('airflow', '/opt/airflow')
    time_stamp: datetime = kwargs['date']

    airflow = Path(airflow_dir)
    reports_directory = airflow / "data-export-files/oclc/reports"
    output: dict = {}

    for library, report in libraries_reports.items():
        reports_path = reports_directory / library / name
        reports_path.mkdir(parents=True, exist_ok=True)
        report_path = reports_path / f"{time_stamp.isoformat()}.html"
        report_path.write_text(report)
        logger.info(f"Created {name} report for {library} at {report_path}")
        output[library] = str(report_path)

    return output


@task
def filter_failures_task(update_failures, match_failures, new_failures) -> dict:
    filtered_errors: dict = dict()

    return filtered_errors


@task
def holdings_set_errors_task(**kwargs):
    failures = kwargs['failures']


@task
def multiple_oclc_numbers_task(**kwargs):
    task_instance = kwargs['ti']

    new_multiple_records = task_instance.xcom_pull(
        task_ids='divide_new_records_by_library'
    )
    deletes_multiple_records = task_instance.xcom_pull(
        task_ids='divide_delete_records_by_library'
    )
    kwargs['all_multiple_codes'] = new_multiple_records + deletes_multiple_records

    kwargs['folio_url'] = Variable.get("FOLIO_URL")

    return _generate_multiple_oclc_numbers_report(**kwargs)
