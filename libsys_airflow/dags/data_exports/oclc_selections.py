import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG

from airflow.decorators import task, task_group
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.data_exports.instance_ids import (
    choose_fetch_folio_ids,
    fetch_record_ids,
    save_ids_to_fs,
)

from libsys_airflow.plugins.data_exports.marc.oclc import (
    archive_instanceid_csv,
    filter_updates,
)

from libsys_airflow.plugins.data_exports.marc.exports import marc_for_instances

from libsys_airflow.plugins.data_exports.marc.transforms import (
    divide_into_oclc_libraries,
    remove_marc_files,
)
from libsys_airflow.plugins.data_exports.email import (
    generate_multiple_oclc_identifiers_email,
    generate_no_holdings_instances_email,
)

from libsys_airflow.plugins.data_exports.oclc_reports import (
    multiple_oclc_numbers_task,
    no_holdings_task,
)

logger = logging.getLogger(__name__)

devs_to_email_addr = Variable.get("EMAIL_DEVS")

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email": [devs_to_email_addr],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

pacific_timezone = ZoneInfo("America/Los_Angeles")

with DAG(
    "select_oclc_records",
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        cron=Variable.get("select_oclc", "30 1 * * *"), timezone="America/Los_Angeles"
    ),
    tags=["data export", "oclc"],
    params={
        "from_date": Param(
            f"{(datetime.now(pacific_timezone) - timedelta(1)).strftime('%Y-%m-%d')}",
            format="date",
            type="string",
            description="The earliest date to select record IDs from FOLIO.",
        ),
        "to_date": Param(
            f"{(datetime.now(pacific_timezone)).strftime('%Y-%m-%d')}",
            format="date",
            type="string",
            description="The latest date to select record IDs from FOLIO.",
        ),
        "fetch_folio_record_ids": Param(True, type="boolean"),
        "saved_record_ids_kind": Param(None, type=["null", "string"]),
    },
    render_template_as_native_obj=True,
    start_date=datetime(2025, 1, 14),
    catchup=False,
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

    filter_out_updates_ids = PythonOperator(
        task_id="filters_updates_ids",
        python_callable=filter_updates,
        op_kwargs={
            "all_records_ids": "{{ ti.xcom_pull(task_ids='fetch_record_ids_from_folio') }}"
        },
    )

    save_ids_to_file = PythonOperator(
        task_id="save_ids_to_file",
        trigger_rule="none_failed_min_one_success",
        python_callable=save_ids_to_fs,
        op_kwargs={
            "vendor": "oclc",
            "record_id_kind": "{{ params.saved_record_ids_kind }}",
            "upstream_task_id": "filters_updates_ids",
        },
    )

    archive_csv = PythonOperator(
        task_id="archive_instance_ids_csv",
        python_callable=archive_instanceid_csv,
        op_kwargs={
            "instance_id_csvs": "{{ ti.xcom_pull(task_ids='save_ids_to_file') }}"
        },
    )

    @task(multiple_outputs=True)
    def retrieve_marc_records(**kwargs):
        ti = kwargs.get("ti")
        instance_files = ti.xcom_pull(task_ids="save_ids_to_file")
        return marc_for_instances(instance_files=instance_files)

    @task
    def divide_new_records_by_library(**kwargs):
        task_instance = kwargs["ti"]
        new_records = kwargs.get("new_records", [])
        missing_holdings, divided_records = divide_into_oclc_libraries(
            marc_file_list=new_records
        )
        task_instance.xcom_push(key="missing_holdings", value=missing_holdings)
        return divided_records

    @task
    def divide_delete_records_by_library(**kwargs):
        task_instance = kwargs["ti"]
        deleted_records = kwargs.get("deleted_records", [])
        missing_holdings, divided_records = divide_into_oclc_libraries(
            marc_file_list=deleted_records
        )
        task_instance.xcom_push(key="missing_holdings", value=missing_holdings)
        return divided_records

    @task
    def divide_updates_records_by_library(**kwargs):
        task_instance = kwargs["ti"]
        updates_records = kwargs.get("updates_records", [])
        missing_holdings, divided_records = divide_into_oclc_libraries(
            marc_file_list=updates_records
        )
        task_instance.xcom_push(key="missing_holdings", value=missing_holdings)
        return divided_records

    @task_group(group_id="multiple-oclc-numbers-group")
    def multiple_oclc_numbers_group(**kwargs):
        kwargs["reports"] = multiple_oclc_numbers_task(**kwargs)
        generate_multiple_oclc_identifiers_email(**kwargs)

    @task_group(group_id="no-holdings-instances-group")
    def no_holdings_for_instances_group(**kwargs):
        kwargs["report"] = no_holdings_task(**kwargs)
        generate_no_holdings_instances_email(**kwargs)

    @task
    def remove_original_marc_files(**kwargs):
        marc_file_list = kwargs["marc_file_list"]
        remove_marc_files(marc_file_list['new'])
        remove_marc_files(marc_file_list['updates'])
        remove_marc_files(marc_file_list['deletes'])

    fetch_marc_records = retrieve_marc_records()

    new_records_by_library = divide_new_records_by_library(
        new_records=fetch_marc_records["new"]  # type: ignore
    )

    delete_records_by_library = divide_delete_records_by_library(
        deleted_records=fetch_marc_records["deletes"]  # type: ignore
    )

    updates_records_by_library = divide_updates_records_by_library(
        updates_records=fetch_marc_records["updates"]  # type: ignore
    )

    finish_division = EmptyOperator(task_id="finish_division")

    multiple_oclc_numbers = multiple_oclc_numbers_group()

    no_holdings_instances = no_holdings_for_instances_group()

    remove_original_marc = remove_original_marc_files(marc_file_list=fetch_marc_records)

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )


check_record_ids >> fetch_folio_record_ids >> filter_out_updates_ids >> save_ids_to_file
check_record_ids >> save_ids_to_file >> fetch_marc_records

(
    fetch_marc_records
    >> [
        new_records_by_library,
        delete_records_by_library,
        updates_records_by_library,
    ]
    >> finish_division
)

(
    finish_division
    >> [no_holdings_instances, multiple_oclc_numbers, remove_original_marc, archive_csv]
    >> finish_processing_marc
)
