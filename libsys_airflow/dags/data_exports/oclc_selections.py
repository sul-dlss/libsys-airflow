from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from libsys_airflow.plugins.data_exports.instance_ids import (
    fetch_record_ids,
    save_ids_to_fs,
)

from libsys_airflow.plugins.data_exports.marc.exports import marc_for_instances

from libsys_airflow.plugins.data_exports.marc.transforms import (
    divide_into_oclc_libraries,
    remove_fields_from_marc_files,
)
from libsys_airflow.plugins.data_exports.email import (
    generate_multiple_oclc_identifiers_email,
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
    schedule=timedelta(
        days=int(Variable.get("schedule_oclc_days", 7)),
        hours=int(Variable.get("schedule_oclc_hours", 7)),
    ),
    start_date=datetime(2024, 2, 25),
    catchup=False,
    tags=["data export"],
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
    },
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
        op_kwargs={
            "instance_files": "{{ ti.xcom_pull('save_ids_to_file') }}",
        },
    )

    transform_marc_fields = PythonOperator(
        task_id="transform_folio_remove_marc_fields",
        python_callable=remove_fields_from_marc_files,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull(task_ids='fetch_marc_records_from_folio') }}"
        },
    )

    divide_marc_records_by_library = PythonOperator(
        task_id="divide_marc_records_by_library",
        python_callable=divide_into_oclc_libraries,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull(task_ids='fetch_marc_records_from_folio') }}"
        },
    )

    send_multiple_oclc_codes_email = PythonOperator(
        task_id="multiple_oclc_codes_email",
        python_callable=generate_multiple_oclc_identifiers_email,
        op_kwargs={
            "multiple_codes": "{{ ti.xcom_pull(task_ids='divide_marc_records_by_library', key='multiple-oclc-codes')}}"
        },
    )

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )


fetch_folio_record_ids >> save_ids_to_file >> fetch_marc_records
fetch_marc_records >> transform_marc_fields >> divide_marc_records_by_library
(
    divide_marc_records_by_library
    >> send_multiple_oclc_codes_email
    >> finish_processing_marc
)
