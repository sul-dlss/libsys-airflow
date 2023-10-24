import logging
import pathlib

from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from libsys_airflow.plugins.folio.folio_client import FolioClient

from libsys_airflow.plugins.orafin.emails import (
    generate_summary_email,
    generate_excluded_email,
)


from libsys_airflow.plugins.orafin.reports import (
    filter_files,
)

from libsys_airflow.plugins.orafin.payments import (
    generate_file,
    get_invoice,
    init_feeder_file,
    models_converter,
    transfer_to_orafin,
)


logger = logging.getLogger(__name__)


def _folio_client():
    try:
        return FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )
    except ValueError as error:
        logger.error(error)
        raise


@task
def consolidate_reports_task(ti=None):
    existing_reports = ti.xcom_pull(
        task_ids="filter_files_task", key="existing_reports"
    )
    new_reports = ti.xcom_pull(task_ids="filter_files_task", key="new_reports")
    all_files = []
    for row in existing_reports + new_reports:
        all_files.append(row)
    logger.info(f"Consolidated {all_files}")
    return all_files


@task
def email_excluded_task(invoices: list):
    folio_url = Variable.get("FOLIO_URL")
    if len(invoices) > 0:
        generate_excluded_email(invoices, folio_url)
    return f"Emailed report for {len(invoices):,} invoices"


@task
def email_summary_task(invoices: list):
    folio_url = Variable.get("FOLIO_URL")
    generate_summary_email(invoices, folio_url)
    return f"Emailed summary report for {len(invoices):,} invoices"


@task
def filter_files_task(ti=None):
    ls_output = ti.xcom_pull(task_ids="find_files")
    existing_reports, new_reports = filter_files(ls_output)
    ti.xcom_push(key="existing_reports", value=existing_reports)
    ti.xcom_push(key="new_reports", value=new_reports)


@task(multiple_outputs=True)
def filter_invoices_task(invoices: list):
    feeder_file, excluded = [], []
    for row in invoices:
        if row['exclude'] is True:
            excluded.append(row['invoice'])
        else:
            feeder_file.append(row['invoice'])
    return {"feed": feeder_file, "excluded": excluded}


@task
def get_new_reports_task(ti=None):
    return ti.xcom_pull(task_ids="filter_files_task", key="new_reports")


@task
def launch_report_processing_task(**kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    ti = kwargs.get("ti")
    new_reports = ti.xcom_pull(task_ids="filter_files_task", key="new_reports")
    for i, report in enumerate(new_reports):
        TriggerDagRunOperator(
            task_id=f"ap_payment_report-{i}",
            trigger_dag_id="ap_payment_report",
            conf={
                "ap_report_path": f"{airflow}/orafin-files/reports/{report['file_name']}"
            },
        ).execute(kwargs)


@task(max_active_tis_per_dag=5)
def transform_folio_data_task(invoice_id: str):
    """
    Takes Invoice ID and retrieves invoice information and tax status
    from the invoice's organization
    """
    folio_client = _folio_client()
    converter = models_converter()
    # Call to Okapi invoice endpoint
    invoice, exclude = get_invoice(invoice_id, folio_client, converter)
    return {"invoice": converter.unstructure(invoice), "exclude": exclude}


@task
def feeder_file_task(invoices: list):
    converter = models_converter()
    folio_client = _folio_client()
    feeder_file = init_feeder_file(invoices, folio_client, converter)

    return converter.unstructure(feeder_file)


@task
def generate_feeder_file_task(feeder_file: dict, airflow: str = "/opt/airflow") -> str:
    # Initialize Feeder File Task
    folio_client = _folio_client()
    orafin_path = pathlib.Path(f"{airflow}/orafin-files/data")
    orafin_path.mkdir(exist_ok=True, parents=True)
    feeder_file_instance = generate_file(feeder_file, folio_client)
    feeder_file_path = orafin_path / feeder_file_instance.file_name
    with feeder_file_path.open("w+") as fo:
        fo.write(feeder_file_instance.generate())
    logger.info(f"Feeder-file {feeder_file_path.resolve()}")
    return str(feeder_file_path.resolve())


# @task -- When SFTP is available on AP server, uncomment this line to make a taskflow task
def sftp_file_task(feeder_file_path: str, sftp_connection: str = None):  # type: ignore
    bash_operator = transfer_to_orafin(feeder_file_path)
    return bash_operator
