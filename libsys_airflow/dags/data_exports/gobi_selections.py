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

from libsys_airflow.plugins.data_exports.marc.gobi import gobi_list_from_marc_files

from libsys_airflow.plugins.data_exports.marc.exports import marc_for_instances

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    "select_gobi_records",
    default_args=default_args,
    schedule=timedelta(
        days=int(Variable.get("schedule_gobi_days", 7)),
        hours=int(Variable.get("schedule_gobi_hours", 3)),
    ),
    start_date=datetime(2024, 2, 26),
    catchup=False,
    tags=["data export", "gobi"],
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
        trigger_rule="none_failed_min_one_success",
        python_callable=save_ids_to_fs,
        op_kwargs={
            "vendor": "gobi",
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

    generate_isbn_list = PythonOperator(
        task_id="generate_isbn_lists",
        python_callable=gobi_list_from_marc_files,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull('fetch_marc_records_from_folio') }}"
        },
    )

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )


check_record_ids >> [fetch_folio_record_ids, save_ids_to_file]
fetch_folio_record_ids >> save_ids_to_file >> fetch_marc_records
save_ids_to_file >> fetch_marc_records

fetch_marc_records >> generate_isbn_list >> finish_processing_marc
