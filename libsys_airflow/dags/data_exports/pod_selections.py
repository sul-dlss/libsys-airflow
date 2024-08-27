import pathlib

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from libsys_airflow.plugins.data_exports.instance_ids import (
    choose_fetch_folio_ids,
    fetch_record_ids,
    save_ids_to_fs,
)

from libsys_airflow.plugins.data_exports.marc.exports import marc_for_instances
from libsys_airflow.plugins.data_exports.marc.transforms import (
    add_holdings_items_to_marc_files,
    change_leader_for_deletes,
    clean_and_serialize_marc_files,
    zip_marc_file,
)

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def compress_marc_files(marc_files: list):
    for marc_file in marc_files:
        stem = pathlib.Path(marc_file).suffix
        xml = marc_file.replace(stem, '.xml')
        zip_marc_file(xml)


with DAG(
    "select_pod_records",
    default_args=default_args,
    schedule=timedelta(
        days=int(Variable.get("schedule_pod_days", 1)),
        hours=int(Variable.get("schedule_pod_hours", 2)),
    ),
    start_date=datetime(2024, 2, 26),
    catchup=False,
    tags=["data export", "pod"],
    params={
        "from_date": Param(
            f"{datetime.now().strftime('%Y-%m-%d')}",
            format="date",
            type="string",
            description="The earliest date to select record IDs from FOLIO.",
        ),
        "to_date": Param(
            f"{(datetime.now() + timedelta(1)).strftime('%Y-%m-%d')}",
            format="date",
            type="string",
            description="The latest date to select record IDs from FOLIO.",
        ),
        "fetch_folio_record_ids": Param(True, type="boolean"),
        "saved_record_ids_kind": Param(None, type=["null", "string"]),
    },
    render_template_as_native_obj=True,
) as dag:
    check_record_ids = BranchPythonOperator(
        task_id="check_record_ids",
        python_callable=choose_fetch_folio_ids,
        op_kwargs={"fetch_folio_record_ids": "{{ params.fetch_folio_record_ids }}"},
    )

    fetch_folio_record_ids = PythonOperator(
        task_id="fetch_record_ids_from_folio",
        python_callable=fetch_record_ids,
    )

    save_ids_to_file = PythonOperator(
        task_id="save_ids_to_file",
        python_callable=save_ids_to_fs,
        trigger_rule="none_failed_min_one_success",
        op_kwargs={
            "vendor": "pod",
            "record_id_kind": "{{ params.saved_record_ids_kind }}",
        },
    )

    fetch_marc_records = PythonOperator(
        task_id="fetch_marc_records_from_folio",
        python_callable=marc_for_instances,
        op_kwargs={
            "instance_files": "{{ ti.xcom_pull('save_ids_to_file') }}",
        },
    )

    transform_marc_record = PythonOperator(
        task_id="transform_folio_marc_record",
        python_callable=add_holdings_items_to_marc_files,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull('fetch_marc_records_from_folio') }}",
            "full_dump": False,
        },
    )

    transform_marc_fields = PythonOperator(
        task_id="transform_folio_marc_clean_serialize",
        python_callable=clean_and_serialize_marc_files,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull('fetch_marc_records_from_folio') }}"
        },
    )

    transform_leader_fields = PythonOperator(
        task_id="transform_folio_modify_leader_fields",
        python_callable=change_leader_for_deletes,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull('fetch_marc_records_from_folio') }}"
        },
    )

    transform_compress_marc_files = PythonOperator(
        task_id="transform_folio_marc_compress_files",
        python_callable=compress_marc_files,
        op_kwargs={"marc_files": "{{ ti.xcom_pull('fetch_marc_records_from_folio') }}"},
    )

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )


check_record_ids >> [fetch_folio_record_ids, save_ids_to_file]
fetch_folio_record_ids >> save_ids_to_file >> fetch_marc_records
save_ids_to_file >> fetch_marc_records

fetch_marc_records >> transform_marc_record >> transform_marc_fields
transform_marc_fields >> transform_leader_fields >> transform_compress_marc_files
transform_compress_marc_files >> finish_processing_marc
