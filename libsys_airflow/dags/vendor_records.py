from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

from plugins.vendors import (
    backup_retrieved_files,
    check_retrieve_files,
    zip_extraction
)

with DAG(
    "vendor_records",
    schedule_interval=None,
    start_date=datetime(2023, 4, 11),
    catchup=False,
) as dag:
    
    context = get_current_context()

    check_retrieve_file = BranchPythonOperator(
        task_id="check-count-file",
        python_callable=check_retrieve_files,
        op_kwargs={
            "vendor": context.get("params")
            "default_task": "",
            "marc_pre_processing": "",
            "invoice_delay": "delay-invoice-processing"
        },
    )

    delay_invoice_file = EmptyOperator(
        task_id="delay-invoice-processing",
    )


    extract_zipfiles = PythonOperator(
        task_id="extract-zipfiles",
        python_callable=zip_extraction
    )


    make_backups = PythonOperator(
        task_id="file-backups",
        python_callable=backup_retrieved_files
    )

    [check_count_file, check_invoice_file, check_for_invoice_file] >> extract_zipfiles
    extract_zipfiles >> make_backups
