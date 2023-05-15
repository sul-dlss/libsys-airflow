"""Remediation of Failed Migration Loads into FOLIO"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from textwrap import dedent

from folioclient import FolioClient
from libsys_airflow.plugins.folio.audit import setup_audit_db, audit_instance_views
from libsys_airflow.plugins.folio.remediate import add_missing_records, start_record_qa
from libsys_airflow.plugins.folio.reports import inventory_audit_report

folio_client = FolioClient(
    Variable.get("okapi_url"),
    "sul",
    Variable.get("folio_user"),
    Variable.get("folio_password"),
)

default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "audit_fix_record_loads",
    schedule_interval=None,
    default_args=default_args,
    start_date=datetime(2022, 3, 28),
    catchup=False,
    max_active_runs=4,
    tags=["bib_import"],
) as dag:
    dag.doc = dedent("""# Audit and Remediation DAG""")

    start_check_add = PythonOperator(
        task_id="start-check-add", python_callable=start_record_qa
    )

    init_audit_remediation_db = PythonOperator(
        task_id='init-audit-fix-db',
        python_callable=setup_audit_db,
        op_kwargs={
            "iteration_id": "{{ task_instance.xcom_pull(task_ids='start-check-add')}}"
        },
    )

    instance_views_audit = PythonOperator(
        task_id="instance-views-audit",
        python_callable=audit_instance_views,
        op_kwargs={
            "iteration_id": "{{ ti.xcom_pull(task_ids='start-check-add') }}",
        },
    )

    remediate_missing_records = PythonOperator(
        task_id="remediate-missing-records",
        python_callable=add_missing_records,
        op_kwargs={
            "iteration_id": "{{ ti.xcom_pull(task_ids='start-check-add') }}",
            "folio_client": folio_client,
        },
    )

    generate_report = PythonOperator(
        task_id="inventory-report",
        python_callable=inventory_audit_report,
        op_kwargs={"iteration_id": "{{ ti.xcom_pull(task_ids='start-check-add') }}"},
    )

    finished = EmptyOperator(task_id="finished-errors-handling")

    start_check_add >> init_audit_remediation_db
    init_audit_remediation_db >> instance_views_audit
    instance_views_audit >> remediate_missing_records
    remediate_missing_records >> generate_report >> finished
