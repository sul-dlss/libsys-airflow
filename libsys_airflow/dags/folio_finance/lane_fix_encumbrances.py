from datetime import datetime, timedelta

from airflow.sdk import DAG, Param, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.standard.operators.python import PythonOperator

from libsys_airflow.plugins.folio.encumbrances.fix_encumbrances_run import (
    fix_encumbrances_run,
)
from libsys_airflow.plugins.folio.encumbrances.email import email_log


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

FY_CODE = Variable.get("FISCAL_YEAR_CODE_LANE", "LANE2025")


with DAG(
    "fix_encumbrances_lane",
    default_args=default_args,
    schedule=CronDataIntervalTimetable(
        cron=Variable.get("fix_encumbrances_lane", "00 2 * * MON"),
        timezone="America/Los_Angeles",
    ),
    start_date=datetime(2024, 8, 29),
    catchup=False,
    tags=["folio"],
    params={
        "choice": Param(
            1,
            type="integer",
            minimum=1,
            maximum=9,
            description="The fix_encumbrance script choice. See libsys_airflow/plugins/folio/fix_encumbrances.py:892",
        ),
    },
) as dag:

    start = EmptyOperator(task_id='start')

    run_fix_encumbrances = PythonOperator(
        task_id="run_fix_encumbrances_script",
        python_callable=fix_encumbrances_run,
        op_args=[
            "{{ params.choice }}",
            FY_CODE,
            Variable.get("TENANT_ID", "sul"),
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        ],
        op_kwargs={"library": "lane"},
    )

    send_logs = PythonOperator(
        task_id="email_fix_encumbrances_log",
        python_callable=email_log,
        trigger_rule='all_done',
        op_kwargs={
            "log_file": "{{ ti.xcom_pull('run_fix_encumbrances_script') }}",
            "fy_code": FY_CODE,
        },
    )

    end = EmptyOperator(task_id='stop')


start >> run_fix_encumbrances >> send_logs >> end
