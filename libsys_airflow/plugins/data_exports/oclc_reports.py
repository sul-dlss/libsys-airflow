import logging

from datetime import datetime, UTC
from pathlib import Path

from airflow.decorators import task
from airflow.models import Variable
from jinja2 import Template

logger = logging.getLogger(__name__)

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


def _generate_multiple_oclc_numbers_report(**kwargs) -> dict:
    airflow_dir: str = kwargs.get('airflow', '/opt/airflow')
    airflow = Path(airflow_dir)
    dag_run: dict = kwargs['dag_run']
    multiple_codes: list = kwargs['all_multiple_codes']
    folio_base_url: str = kwargs['folio_url']
    date: datetime = kwargs.get('date', datetime.now(UTC))

    def _folio_url(instance_uuid: dict):
        return f"{folio_base_url}/inventory/view/{instance_uuid}"

    reports: dict = {}
    report_template = Template(multiple_oclc_numbers_template)

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
            info['folio_url'] = _folio_url(uuid)
            info['uuid'] = uuid
        reports[library] = report_template.render(
            dag_run=dag_run,
            date=date.strftime("%d %B %Y"),
            library=library,
            instances=instances,
        )

    reports_path = airflow / "data-export-files/oclc/reports"

    return _save_reports(
        name="multiple_oclc_numbers",
        reports=reports,
        reports_path=reports_path,
        date=date,
    )


def _save_reports(**kwargs) -> dict:
    name: str = kwargs['name']
    libraries_reports: dict = kwargs['reports']
    reports_directory: Path = kwargs['reports_path']
    time_stamp: datetime = kwargs['date']

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
