from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from libsys_airflow.plugins.folio.rag import (
    process_folio_instances,
    FETCH_INSTANCES_SQL,
)

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

VECTOR_STORE_CONN = "your_vector_store"  # configure in Airflow connections


with DAG(
    dag_id="folio_rag_pipeline",
    default_args=default_args,
    description="Extract, verbalize, embed, and store FOLIO instance records for RAG",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["library", "rag", "folio"],
) as dag:

    fetch_folio_instances = SQLExecuteQueryOperator(
        task_id="fetch_folio_instances",
        conn_id="postgres_folio",
        sql=FETCH_INSTANCES_SQL,
        do_xcom_push=True,
    )

    process_instances = process_folio_instances()

    fetch_folio_instances >> process_instances