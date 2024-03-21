from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import PythonOperator

# from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from libsys_airflow.plugins.data_exports.full_dump_ids import (
    fetch_full_dump_ids,
    refresh_view,
)

from libsys_airflow.plugins.data_exports.marc.exports import marc_for_instances

from libsys_airflow.plugins.data_exports.marc.transforms import (
    add_holdings_items_to_marc_files,
    remove_marc_fields,
)

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "select_all_records",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["data export"],
) as dag:
    refresh_table_view = PythonOperator(
        task_id="refresh_materialized_table_view",
        python_callable=refresh_view,
    )

    fetch_and_save_folio_record_ids = PythonOperator(
        task_id="fetch_record_ids_from_folio",
        python_callable=fetch_full_dump_ids,
        op_kwargs={"batch_size": 50000},
    )

    fetch_marc_records = PythonOperator(
        task_id="fetch_marc_records_from_folio",
        python_callable=marc_for_instances,
        op_kwargs={"vendor": "full-dump"},
    )

    transform_marc_record = PythonOperator(
        task_id="transform_folio_marc_record",
        python_callable=add_holdings_items_to_marc_files,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull('fetch_marc_records_from_folio') }}"
        },
    )

    transform_marc_fields = PythonOperator(
        task_id="transform_folio_remove_marc_fields",
        python_callable=remove_marc_fields,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull('fetch_marc_records_from_folio') }}"
        },
    )

    # send_to_vendor = TriggerDagRunOperator(
    #     task_id="send_full_dump_records",
    #     trigger_dag_id="send_full_dump_records",
    # )

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )


refresh_table_view >> fetch_and_save_folio_record_ids >> fetch_marc_records
fetch_marc_records >> transform_marc_record >> transform_marc_fields
transform_marc_fields >>  finish_processing_marc
# transform_marc_fields >> send_to_vendor >> finish_processing_marc
