from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from plugins.vendors import (
    backup_retrieved_files,
    check_retrieve_files,
    rename_vendor_files,
    zip_extraction
)

with DAG(
    "gobi",
    schedule_interval=None,
    start_date=datetime(2023, 4, 11),
    catchup=False,
) as dag:
    

    handle_retrieved_file = PythonOperator(
        task_id="check-count-file",
        python_callable=check_retrieve_files,
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

    rename_files = PythonOperator(
        task_id="rename-files",
        python_callable=rename_vendor_files
    )


    update_migration = EmptyOperator(
        task_id="marc-record-updates"
    )

    with TaskGroup(group_id="mod-data-import") as mod_data_import:

        batch_records = EmptyOperator(
            task_id="batch-records-for-import"
        )

        create_upload_definition = EmptyOperator(
            task_id="create-upload-def"
        )

        file_processing_post = EmptyOperator(
            task_id="file-posting"
        )

        batch_records >> create_upload_definition >> file_processing_post

    send_bibs_to_backstage = EmptyOperator(
        task_id="send-bibs-to-backstage"
    )


    with TaskGroup(group_id="reporting") as reporting:
        start_reporting = EmptyOperator(
            task_id="start-reporting"
        )

        generate_load_report = EmptyOperator(
            task_id="load-report-generation"
        )

        hrid_list_report = EmptyOperator(
            task_id="instance-hrid-list-report"

        )

        end_reporting = EmptyOperator(
            task_id="end-reporting"
        )

        start_reporting >> [generate_load_report, hrid_list_report] >> end_reporting

    email_notifications = EmptyOperator(
        task_id="notification-emails"
    )

    handle_retrieved_file >> [extract_zipfiles, delay_invoice_file]
    extract_zipfiles >> make_backups >> rename_files
    rename_files >> update_migration >> mod_data_import
    mod_data_import >> [send_bibs_to_backstage, reporting]
    reporting >> email_notifications
