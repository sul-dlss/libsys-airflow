import logging
from datetime import datetime, timedelta

from airflow import DAG

from airflow.decorators import task
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
    remove_marc_files,
)
from libsys_airflow.plugins.data_exports.email import (
    generate_multiple_oclc_identifiers_email,
)

logger = logging.getLogger(__name__)

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

    transform_marc_fields = PythonOperator(
        task_id="transform_folio_remove_marc_fields",
        python_callable=remove_fields_from_marc_files,
        op_kwargs={
            "marc_file_list": "{{ ti.xcom_pull(task_ids='retrieve_marc_records') }}"
        },
    )

    @task(multiple_outputs=True)
    def retrieve_marc_records(**kwargs):
        ti = kwargs.get("ti")
        instance_files = ti.xcom_pull(task_ids="save_ids_to_file")
        logger.info(f"Instance files {instance_files} {type(instance_files)}")
        return marc_for_instances(instance_files=instance_files)

    @task
    def divide_new_records_by_library(**kwargs):
        new_records = kwargs.get("new_records", [])
        return divide_into_oclc_libraries(marc_file_list=new_records)

    @task
    def divide_delete_records_by_library(**kwargs):
        deleted_records = kwargs.get("deleted_records", [])
        return divide_into_oclc_libraries(marc_file_list=deleted_records)

    @task
    def aggregate_email_multiple_records(**kwargs):
        ti = kwargs["ti"]
        new_multiple_records = ti.xcom_pull(task_ids='divide_new_records_by_library')
        deletes_multiple_records = ti.xcom_pull(
            task_ids='divide_delete_records_by_library'
        )
        all_multiple_records = new_multiple_records + deletes_multiple_records
        generate_multiple_oclc_identifiers_email(all_multiple_records)

    @task
    def remove_original_marc_files(**kwargs):
        marc_file_list = kwargs["marc_file_list"]
        remove_marc_files(str(marc_file_list['new']))
        remove_marc_files(str(marc_file_list['deletes']))

    fetch_marc_records = retrieve_marc_records()

    new_records_by_library = divide_new_records_by_library(
        new_records=fetch_marc_records["new"]  # type: ignore
    )

    delete_records_by_library = divide_delete_records_by_library(
        deleted_records=fetch_marc_records["deletes"]  # type: ignore
    )

    finish_division = EmptyOperator(task_id="finish_division")

    remove_original_marc = remove_original_marc_files(marc_file_list=fetch_marc_records)

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )


fetch_folio_record_ids >> save_ids_to_file >> fetch_marc_records
(
    fetch_marc_records
    >> transform_marc_fields
    >> [new_records_by_library, delete_records_by_library]
)
[new_records_by_library, delete_records_by_library] >> finish_division
(
    finish_division
    >> [aggregate_email_multiple_records(), remove_original_marc]
    >> finish_processing_marc
)
