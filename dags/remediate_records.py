"""Remediation of Failed Migration Loads into FOLIO"""
from datetime import datetime, timedelta
import logging

from airflow import DAG

from airflow.operators.dummy import DummyOperator

from airflow.operators.python import PythonOperator

from textwrap import dedent

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

    instances_notification = DummyOperator(task_id="instances-notification")

    holdings_notification = DummyOperator(task_id="holdings-notification")

    items_notification = DummyOperator(task_id="items-notification")

    instances_notification >> holdings_notification
    holdings_notification >> items_notification

