from datetime import datetime, timedelta

from airflow.sdk import (
    DAG,
    Param,
    Variable,
)
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.data_exports.instance_ids import (
    choose_fetch_folio_ids,
    fetch_record_ids,
    save_ids_to_fs,
)

from libsys_airflow.plugins.data_exports.marc.exports import marc_for_instances

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


with DAG(
    "select_backstage_records",
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        cron=Variable.get("select_backstage", "30 22 * * FRI"),
        timezone="America/Los_Angeles",
    ),
    start_date=datetime(2024, 11, 18),
    catchup=False,
    tags=["data export", "backstage"],
    params={
        "from_date": Param(
            f"{(datetime.now() - timedelta(1)).strftime('%Y-%m-%d')}",
            format="date",
            type="string",
            description="The earliest date to select record IDs from FOLIO.",
        ),
        "to_date": Param(
            f"{(datetime.now()).strftime('%Y-%m-%d')}",
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
        op_kwargs={"record_kind": ["new"]},
    )

    save_ids_to_file = PythonOperator(
        task_id="save_ids_to_file",
        python_callable=save_ids_to_fs,
        trigger_rule="none_failed_min_one_success",
        op_kwargs={
            "vendor": "backstage",
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

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )


check_record_ids >> [fetch_folio_record_ids, save_ids_to_file]
fetch_folio_record_ids >> save_ids_to_file >> fetch_marc_records
save_ids_to_file >> fetch_marc_records >> finish_processing_marc
