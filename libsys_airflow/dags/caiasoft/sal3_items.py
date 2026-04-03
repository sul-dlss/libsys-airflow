from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.sdk import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.data_exports.sql_pool import SQLPool
from libsys_airflow.plugins.data_exports.sal3_items import (
    create_sal3_items_view,
    folio_items_to_csv,
    gather_items_csv_for_caiasoft,
    pick_success_files,
)
from libsys_airflow.plugins.data_exports.transmission_tasks import (
    archive_transmitted_data_task,
    transmit_data_ftp_task,
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

"""
Sends a report of items or holdings with SAL3 locations to Caiasoft so they can do audits
"""
with DAG(
    "caiasoft_sal3_items_export",
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        cron=Variable.get("sal3_caiasoft", "03 12 24 * *"),
        timezone="America/Los_Angeles",
    ),
    start_date=datetime(2025, 4, 9),
    catchup=False,
    tags=["caiasoft"],
    render_template_as_native_obj=True,
    params={},
) as dag:
    connection_pool = SQLPool().pool()

    create_sal3_materialized_view = PythonOperator(
        task_id="sal3_items_materialized_view",
        python_callable=create_sal3_items_view,
    )

    items_to_csv = PythonOperator(
        task_id="convert_folio_items_query_to_csv",
        python_callable=folio_items_to_csv,
        op_kwargs={
            "connection": connection_pool.getconn(),
        },
    )

    gather_sal3_items_file = PythonOperator(
        task_id="gather_sal3_items_csv",
        python_callable=gather_items_csv_for_caiasoft,
        op_kwargs={
            "sal3_items_csv_path": "{{ ti.xcom_pull('convert_folio_items_query_to_csv') }}",
        },
    )

    send_to_caiasoft = PythonOperator(
        task_id="send_to_caiasoft_ftp",
        python_callable=transmit_data_ftp_task.function,
        op_args=["sftp-caiasoft", "{{ ti.xcom_pull('gather_sal3_items_csv') }}"],
    )

    pick_files = PythonOperator(
        task_id="pick_files_to_archive",
        python_callable=pick_success_files,
        op_args=["{{ ti.xcom_pull('send_to_caiasoft_ftp') }}"],
    )

    archive_file = PythonOperator(
        task_id="archive_csv_file",
        python_callable=archive_transmitted_data_task.function,
        op_args=["{{ ti.xcom_pull('pick_files_to_archive') }}"],
    )


(
    create_sal3_materialized_view
    >> items_to_csv
    >> gather_sal3_items_file
    >> send_to_caiasoft
    >> pick_files
    >> archive_file
)
