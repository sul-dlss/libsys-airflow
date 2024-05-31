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
    add_holdings_items_to_marc_files,
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
    "select_sharevde_records",
    default_args=default_args,
    schedule=timedelta(
        days=int(Variable.get("schedule_sharevde_days", 1)),
        hours=int(Variable.get("schedule_sharevde_hours", 13)),
    ),
    start_date=datetime(2024, 2, 26),
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
        op_kwargs={"vendor": "sharevde"},
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
        task_id="transform_folio_remove_marc_fields",
        python_callable=remove_fields_from_marc_files,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull('fetch_marc_records_from_folio') }}"
        },
    )

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )


fetch_folio_record_ids >> save_ids_to_file >> fetch_marc_records
fetch_marc_records >> transform_marc_record >> transform_marc_fields
transform_marc_fields >> finish_processing_marc
