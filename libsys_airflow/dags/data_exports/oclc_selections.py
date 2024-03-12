from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from libsys_airflow.plugins.data_exports.instance_ids import (
    fetch_record_ids,
    save_ids_to_fs,
)

from libsys_airflow.plugins.data_exports.marc.exports import marc_for_instances

from libsys_airflow.plugins.data_exports.marc.transforms import (
    divide_into_oclc_libraries,
    remove_fields_from_marc_files,
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
    "select_oclc_records",
    default_args=default_args,
    schedule=timedelta(days=int(Variable.get("schedule_oclc_days", 7))),
    start_date=datetime(2024, 2, 25),
    catchup=False,
    tags=["data export"],
) as dag:
    fetch_folio_record_ids = PythonOperator(
        task_id="fetch_record_ids_from_folio",
        python_callable=fetch_record_ids,
    )

    save_ids_to_file = PythonOperator(
        task_id="save_ids_to_file",
        python_callable=save_ids_to_fs,
        op_kwargs={"vendor": "oclc"},
    )

    fetch_marc_records = PythonOperator(
        task_id="fetch_marc_records_from_folio",
        python_callable=marc_for_instances,
        op_kwargs={"vendor": "oclc"},
    )

    transform_marc_fields = PythonOperator(
        task_id="transform_folio_remove_marc_fields",
        python_callable=remove_fields_from_marc_files,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull('fetch_marc_records_from_folio') }}"
        },
    )

    divide_marc_records_by_library = PythonOperator(
        task_id="divide_marc_records_by_library",
        python_callable=divide_into_oclc_libraries,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull('fetch_marc_records_from_folio') }}"
        },
    )

    send_to_vendor = TriggerDagRunOperator(
        task_id="send_oclc_records",
        trigger_dag_id="send_oclc_records",
        conf={"marc_file_list": "{{ ti.xcom_pull('tbd') }}"},
    )

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )


fetch_folio_record_ids >> save_ids_to_file >> fetch_marc_records
fetch_marc_records >> transform_marc_fields >> divide_marc_records_by_library
divide_marc_records_by_library >> send_to_vendor >> finish_processing_marc
