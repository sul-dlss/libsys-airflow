"""Remediation of Failed Migration Loads into FOLIO"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from textwrap import dedent

from folioclient import FolioClient
from plugins.folio.remediate import handle_record_errors

folio_client = FolioClient(
    Variable.get("OKAPI_URL"),
    "sul",
    Variable.get("FOLIO_USER"),
    Variable.get("FOLIO_PASSWORD"),
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
    "fix_failed_record_loads",
    default_args=default_args,
    start_date=datetime(2022, 3, 28),
    catchup=False,
    tags=["bib_import"],
) as dag:
    dag.doc = dedent("""# Remediation DAG""")

    instances_errors = PythonOperator(
        task_id="instances-handler",
        python_callable=handle_record_errors,
        op_kwargs={
            "base": "instance-storage",
            "endpoint": "/inventory/instances",
            "folio_client": folio_client,
        }
    )

    holdings_errors = PythonOperator(
        task_id="holdings-handler",
        python_callable=handle_record_errors,
        op_kwargs={
            "base": "holdings-storage",
            "endpoint": "/holdings-storage/holdings",
            "folio_client": folio_client,
        },
    )

    items_errors = PythonOperator(
        task_id="items-handler",
        python_callable=handle_record_errors,
        op_kwargs={
            "base": "items-storage",
            "endpoint": "/items-storage/items",
            "folio_client": folio_client,
        },
    )

    finished = DummyOperator(task_id="finished-errors-handling")

    instances_errors >> holdings_errors
    holdings_errors >> items_errors >> finished
