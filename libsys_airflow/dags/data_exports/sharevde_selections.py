from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.data_exports.instance_ids import (
    choose_fetch_folio_ids,
    fetch_record_ids,
    save_ids_to_fs,
)

from libsys_airflow.plugins.data_exports.email import send_confirmation_email
from libsys_airflow.plugins.data_exports.marc.exports import marc_for_instances
from libsys_airflow.plugins.data_exports.marc.transforms import (
    add_holdings_items_to_marc_files,
    clean_and_serialize_marc_files,
)

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
    "select_sharevde_records",
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        cron=Variable.get("select_sharevde", "0 20 * * *"),
        timezone="America/Los_Angeles",
    ),
    start_date=datetime(2024, 2, 26),
    catchup=False,
    tags=["data export", "sharevde"],
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
        "user_email": Param(None, type=["null", "string"]),
        "number_of_ids": Param(None, type=["null", "integer"]),
        "uploaded_filename": Param(None, type=["null", "string"]),
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
            "vendor": "sharevde",
            "record_id_kind": "{{ params.saved_record_ids_kind }}",
        },
    )

    email_user = PythonOperator(
        task_id="email_user",
        python_callable=send_confirmation_email,
        op_kwargs={
            "vendor": "sharevde",
            "user_email": "{{ params.user_email }}",
            "record_id_kind": "{{ params.saved_record_ids_kind }}",
            "number_of_ids": "{{ params.number_of_ids }}",
            "uploaded_filename": "{{ params.uploaded_filename }}",
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

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )


check_record_ids >> [fetch_folio_record_ids, save_ids_to_file]
fetch_folio_record_ids >> save_ids_to_file >> fetch_marc_records
save_ids_to_file >> fetch_marc_records
save_ids_to_file >> email_user

fetch_marc_records >> transform_marc_record >> transform_marc_fields
transform_marc_fields >> finish_processing_marc
