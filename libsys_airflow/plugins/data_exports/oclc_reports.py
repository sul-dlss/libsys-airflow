import logging

from datetime import datetime
from pathlib import Path

from airflow.sdk import task, Variable
from jinja2 import DictLoader, Environment

from libsys_airflow.plugins.shared.utils import dag_run_url

logger = logging.getLogger(__name__)

holdings_set_template = """
<h1>OCLC Holdings {% if match %}Matched {% endif %}Set Errors on {{ date }} for {{ library }}</h1>
<p>
  <a href="{{ dag_run_url }}">DAG Run</a>
</p>
<h2>FOLIO Instances that failed trying to set Holdings {% if match %}after successful Match{% endif %}</h2>
<table class="table table-striped">
  <thead>
    <tr>
      <th>Instance</th>
      <th>OCLC Response</th>
    </tr>
  </thead>
  <tbody>
{% for row in failures %}
  <tr>
    <td>
      <a href="{{ folio_url }}/inventory/view/{{ row.uuid }}">{{ row.uuid }}</a>
    </td>
    <td>
    {% if row.context %}
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

holdings_unset_template = """
<h1>OCLC Holdings Unset Errors on {{ date }} for {{ library }}</h1>
<p>
  <a href="{{ dag_run_url }}">DAG Run</a>
</p>
<h2>FOLIO Instances that failed trying to unset Holdings</h2>
<table class="table table-striped">
  <thead>
    <tr>
      <th>Instance</th>
      <th>OCLC Response</th>
    </tr>
  </thead>
  <tbody>
  {% for row in failures %}
  <tr>
    <td>
      <a href="{{ folio_url }}/inventory/view/{{ row.uuid }}">{{ row.uuid }}</a>
    </td>
    <td>
    {% if row.context %}
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

no_holdings_for_instances_template = """
<h1>Instances missing Holdings on {{ date }}</h1>

<p>
 <a href="{{ dag_run_rul }}">DAG Run</a>
</p>

<ol>
{% for uuid in instance_missing_holdings %}
  <li><a href="{{ folio_url }}/inventory/view/{{ uuid }}">{{ uuid }}</a></li>
{% endfor %}
</ol>
"""

multiple_oclc_numbers_template = """
 <h1>Multiple OCLC Numbers on {{ date }} for {{ library }}</h1>

 <p>
  <a href="{{ dag_run_url }}">DAG Run</a>
 </p>

 <h2>FOLIO Instances with Multiple OCLC Numbers</h2>
 <ol>
{% for uuid, instance in failures.items() %}
 <li>
   <a href="{{ folio_url }}/inventory/view/{{ uuid }}">{{ uuid }}</a>:
   <ul>
   {% for num in instance.oclc_numbers %}
    <li>{{ num }}</li>
   {% endfor %}
   </ul>
 </li>
{% endfor %}
  </ol>
"""

new_oclc_invalid_records = """
 {% macro field_table(errors) -%}
  <table class="table table-bordered">
        <thead>
          <tr>
            <th>Tag</th>
            <th>Error Level</th>
            <th>Detail</th>
           </tr>
         </thead>
         <tbody>
         {% for error in errors %}
           <tr>
             <td>{{ error.tag }}</td>
             <td>{{ error.errorLevel }}</td>
             <td>{{ error.message }}</td>
           </tr>
         {% endfor %}
         </tbody>
    </table>
 {% endmacro %}
 <h1>Invalid MARC Records New to OCLC on {{ date }} for {{ library }}</h1>
 <p>
  <a href="{{ dag_run_url }}">DAG Run</a>
 </p>
 <table class="table table-striped">
  <thead>
    <tr>
      <th>Instance</th>
      <th>Reason</th>
      <th>OCLC Response</th>
    </tr>
  </thead>
  <tbody>
  {% for row in failures %}
  <tr>
    <td>
      <a href="{{ folio_url }}/inventory/view/{{ row.uuid }}">{{ row.uuid }}</a>
    </td>
    <td>
      {{ row.reason }} Error Count {{ row.context.errorCount }}
    </td>
    <td>
      <h4>Errors</h4>
      <ol>
      {% for error in row.context.errors %}
        <li>{{ error }}</li>
      {% endfor %}
      </ol>
      <h4>Fixed Field Errors</h4>
       {{ field_table(row.context.fixedFieldErrors) }}
       <h4>Variable Field Errors</h4>
       {{ field_table(row.context.variableFieldErrors) }}
    </td>
   </tr>
  {% endfor %}
  </tbody>
</table>
"""


oclc_payload_template = """<ul>
        <li><strong>Control Number:</strong> {{ row.context.controlNumber }}</li>
        <li><strong>Requested Control Number:</strong> {{ row.context.requestedControlNumber }}</li>
        <li><strong>Institution:</strong>
           <ul>
             <li><em>Code:</em> {{ row.context.institutionCode }}</li>
             <li><em>Symbol:</em> {{ row.context.institutionSymbol }}</li>
           </ul>
        </li>
        <li><strong>First Time Use:</strong> {{ row.context.firstTimeUse }}</li>
        <li><strong>Success:</strong> {{ row.context.success }}</li>
        <li><strong>Message:</strong> {{ row.context.message }}</li>
        <li><strong>Action:</strong> {{ row.context.action }}</li>
     </ul>
"""

jinja_env = Environment(
    loader=DictLoader(
        {
            "holdings-set.html": holdings_set_template,
            "holdings-unset.html": holdings_unset_template,
            "no-instances-holdings.html": no_holdings_for_instances_template,
            "multiple-oclc-numbers.html": multiple_oclc_numbers_template,
            "new-oclc-marc-errors.html": new_oclc_invalid_records,
            "oclc-payload-template.html": oclc_payload_template,
        }
    ),
    autoescape=True,
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


def _generate_holdings_set_report(**kwargs) -> dict:
    date: datetime = kwargs.get('date', datetime.utcnow())

    match = kwargs.get("match", False)

    if date not in kwargs:
        kwargs["date"] = date

    report_dir = "set_holdings"
    kwargs["report_key"] = "Failed to update holdings"
    if match:
        report_dir = "set_holdings_match"
        kwargs["report_key"] = "Failed to update holdings after match"

    kwargs['report_template'] = "holdings-set.html"

    reports = _reports_by_library(**kwargs)

    return _save_reports(
        airflow=kwargs.get('airflow', '/opt/airflow'),
        name=report_dir,
        reports=reports,
        date=date,
    )


def _generate_holdings_unset_report(**kwargs) -> dict:
    date: datetime = kwargs.get('date', datetime.utcnow())

    if date not in kwargs:
        kwargs["date"] = date

    kwargs['report_template'] = "holdings-unset.html"
    kwargs["report_key"] = "Failed holdings_unset"

    reports = _reports_by_library(**kwargs)

    return _save_reports(
        airflow=kwargs.get('airflow', '/opt/airflow'),
        name="unset_holdings",
        reports=reports,
        date=date,
    )


def _generate_missing_holdings_report(**kwargs) -> str:
    date: datetime = kwargs.get('date', datetime.utcnow())
    airflow: str = kwargs.get('airflow', '/opt/airflow')

    airflow_path = Path(airflow)

    if date not in kwargs:
        kwargs["date"] = date

    report_template = jinja_env.get_template("no-instances-holdings.html")

    report = report_template.render(**kwargs)

    missing_reports_path = (
        airflow_path / "data-export-files/oclc/reports/missing_holdings/"
    )
    missing_reports_path.mkdir(parents=True, exist_ok=True)

    report_path = missing_reports_path / f"{date.isoformat()}.html"

    report_path.write_text(report)

    return str(report_path)


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

    template = jinja_env.get_template("multiple-oclc-numbers.html")

    reports: dict = {}

    kwargs["date"] = date.strftime("%d %B %Y")
    kwargs["dag_run_url"] = dag_run_url(dag_run=kwargs["dag_run"])

    for library, errors in library_instances.items():
        kwargs["failures"] = errors
        kwargs["library"] = library

        reports[library] = template.render(**kwargs)

    return _save_reports(
        airflow=kwargs.get('airflow', '/opt/airflow'),
        name="multiple_oclc_numbers",
        reports=reports,
        date=date,
    )


def _generate_new_oclc_invalid_records_report(**kwargs) -> dict:
    date: datetime = kwargs.get('date', datetime.utcnow())

    if date not in kwargs:
        kwargs["date"] = date

    kwargs['report_template'] = "new-oclc-marc-errors.html"
    kwargs["report_key"] = "Failed to add new MARC record"

    reports = _reports_by_library(**kwargs)

    return _save_reports(
        airflow=kwargs.get('airflow', '/opt/airflow'),
        name="new_marc_errors",
        reports=reports,
        date=date,
    )


def _reports_by_library(**kwargs) -> dict:
    failures: dict = kwargs["failures"]
    report_template_name: str = kwargs['report_template']
    report_key: str = kwargs["report_key"]
    date: datetime = kwargs['date']

    reports: dict = dict()

    report_template = jinja_env.get_template(report_template_name)

    for library, rows in failures.items():
        if len(rows) < 1:
            continue
        filtered_failures = []
        for key, errors in rows.items():
            if len(errors) < 1:
                continue
            if key == report_key:
                filtered_failures = errors
        if len(filtered_failures) < 1:
            continue
        kwargs["library"] = library
        kwargs["failures"] = filtered_failures
        kwargs["date"] = date.strftime("%d %B %Y")
        kwargs["dag_run_url"] = dag_run_url(dag_run=kwargs["dag_run"])
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
def holdings_unset_errors_task(**kwargs):
    kwargs['folio_url'] = Variable.get("FOLIO_URL")

    return _generate_holdings_unset_report(**kwargs)


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


@task
def new_oclc_marc_errors_task(**kwargs):
    kwargs['folio_url'] = Variable.get("FOLIO_URL")

    return _generate_new_oclc_invalid_records_report(**kwargs)


@task
def no_holdings_task(**kwargs):
    task_instance = kwargs['ti']

    kwargs['folio_url'] = Variable.get("FOLIO_URL")
    missing_holdings = []
    missing_holdings_new = task_instance.xcom_pull(
        task_ids="divide_new_records_by_library", key="missing_holdings"
    )
    missing_holdings.extend(missing_holdings_new)
    missing_holdings_delete = task_instance.xcom_pull(
        task_ids="divide_delete_records_by_library", key="missing_holdings"
    )
    missing_holdings.extend(missing_holdings_delete)
    missing_holdings_update = task_instance.xcom_pull(
        task_ids="divide_updates_records_by_library", key="missing_holdings"
    )
    missing_holdings.extend(missing_holdings_update)
    kwargs['instance_missing_holdings'] = missing_holdings

    return _generate_missing_holdings_report(**kwargs)
