import logging

from datetime import datetime
from pathlib import Path

from airflow.decorators import task
from airflow.models import Variable
from jinja2 import DictLoader, Environment

logger = logging.getLogger(__name__)

holdings_set_template = """
<h1>OCLC Holdings {% if match %}Matched {% endif %}Set Errors on {{ date }} for {{ library }}</h1>
<p>
  <a href="{{ dag_run.url }}">DAG Run</a>
</p>
<h2>FOLIO Instances that failed trying to set Holdings {% if match %}after successful Match{% endif %}</h2>
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


jinja_env = Environment(
    loader=DictLoader(
        {
            "multiple-oclc-numbers.html": multiple_oclc_numbers_template,
        }
    )
)


def _filter_failures(failures: dict, errors: dict):
    for library, instances in failures.items():
        if library not in errors:
            errors[library] = {}
        if len(instances) < 1:
            continue
        for instance in instances:
            if instance['reason'].startswith("Match failed"):
                continue
            if instance['reason'] in errors[library]:
                errors[library][instance['reason']].append(
                    {"uuid": instance['uuid'], "context": instance['context']}
                )
            else:
                errors[library][instance['reason']] = [
                    {"uuid": instance['uuid'], "context": instance['context']}
                ]


def _folio_url(folio_base_url: str, instance_uuid: dict):
    return f"{folio_base_url}/inventory/view/{instance_uuid}"


def _generate_holdings_set_report(**kwargs) -> dict:
    date: datetime = kwargs.get('date', datetime.utcnow())
    failures: dict = kwargs.pop('failures')

    match = kwargs.get("match", False)

    error_key = "Failed to update holdings"
    report_name = "set_holdings"
    if match:
        report_name = "set_holdings_match"
        error_key = "Failed to update holdings after match"

    kwargs["error_key"] = error_key

    library_instances: dict = {}

    for library_code, errors in failures.items():
        update_holdings_errors = errors.get(error_key, [])
        if len(update_holdings_errors) < 1:
            continue
        if library_code not in library_instances:
            library_instances[library_code] = {}
        for row in update_holdings_errors:
            library_instances[library_code][row['uuid']] = {
                "uuid": row['uuid'],
                "oclc_error": row['context'],
            }

    kwargs['library_instances'] = library_instances
    kwargs['report_template'] = "holdings-set.html"

    reports = _reports_by_library(**kwargs)

    return _save_reports(
        airflow=kwargs.get('airflow', '/opt/airflow'),
        name=report_name,
        reports=reports,
        date=date,
    )


def _generate_multiple_oclc_numbers_report(**kwargs) -> dict:
    multiple_codes: list = kwargs['all_multiple_codes']
    date: datetime = kwargs.get('date', datetime.utcnow())

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

    kwargs['library_instances'] = library_instances
    kwargs['report_template'] = "multiple-oclc-numbers.html"

    reports = _reports_by_library(**kwargs)

    return _save_reports(
        airflow=kwargs.get('airflow', '/opt/airflow'),
        name="multiple_oclc_numbers",
        reports=reports,
        date=date,
    )


def _reports_by_library(**kwargs) -> dict:
    library_instances: dict = kwargs['library_instances']
    folio_base_url: str = kwargs['folio_url']
    report_template_name: str = kwargs['report_template']
    date: datetime = kwargs['date']

    reports: dict = dict()

    report_template = jinja_env.get_template(report_template_name)

    for library, instances in library_instances.items():
        if len(instances) < 1:
            continue
        for uuid, info in instances.items():
            info['folio_url'] = _folio_url(folio_base_url, uuid)
            info['uuid'] = uuid
        kwargs["library"] = library
        kwargs["instances"] = instances
        kwargs["date"] = date.strftime("%d %B %Y")
        reports[library] = report_template.render(**kwargs)

    return reports


def _save_reports(**kwargs) -> dict:
    name: str = kwargs['name']
    libraries_reports: dict = kwargs['reports']
    airflow_dir: str = kwargs['airflow']
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
def filter_failures_task(**kwargs) -> dict:
    def _log_expansion_(fail_dict: dict):
        log = ""
        for lib, errors in fail_dict.items():
            log += f"{lib} - {len(errors)}, "
        return log

    deleted_failures: dict = kwargs["delete"]
    match_failures: dict = kwargs["match"]
    new_failures: dict = kwargs["new"]
    update_failures: dict = kwargs["update"]

    filtered_errors: dict = dict()

    logger.info(f"Update failures: {_log_expansion_(update_failures)}")
    _filter_failures(update_failures, filtered_errors)
    logger.info(f"Deleted failures {_log_expansion_(deleted_failures)}")
    _filter_failures(deleted_failures, filtered_errors)
    logger.info(f"Match failures: {_log_expansion_(match_failures)}")
    _filter_failures(match_failures, filtered_errors)
    logger.info(f"New failures {_log_expansion_(new_failures)}")
    _filter_failures(new_failures, filtered_errors)

    logger.info(filtered_errors)
    return filtered_errors


@task
def holdings_set_errors_task(**kwargs):
    kwargs['folio_url'] = Variable.get("FOLIO_URL")

    return _generate_holdings_set_report(**kwargs)


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
