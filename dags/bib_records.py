"""Imports exported MARC records from Symphony into FOLIO"""

from datetime import datetime, timedelta
import logging

from telnetlib import SUPPRESS_LOCAL_ECHO
from textwrap import dedent
from typing_extensions import TypeAlias  # noqa

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from migration_tools.library_configuration import LibraryConfiguration

from plugins.folio.helpers import archive_artifacts, move_marc_files, process_marc, process_records, tranform_csv_to_tsv
from plugins.folio.holdings import run_holdings_tranformer, post_folio_holding_records
from plugins.folio.login import folio_login
from plugins.folio.instances import post_folio_instance_records, run_bibs_transformer
from plugins.folio.items import run_items_transformer, post_folio_items_records

logger = logging.getLogger(__name__)

sul_config = LibraryConfiguration(
    okapi_url=Variable.get("OKAPI_URL"),
    tenant_id="sul",
    okapi_username=Variable.get("FOLIO_USER"),
    okapi_password=Variable.get("FOLIO_PASSWORD"),
    library_name="Stanford University Libraries",
    base_folder="/opt/airflow/migration",
    log_level_debug=True,
    folio_release="juniper",
    iteration_identifier="",
)


parallel_posts = Variable.get("parallel_posts", 3)

default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "symphony_marc_import",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2022, 1, 3),
    catchup=False,
    tags=["bib_import"],
) as dag:

    dag.doc_md = dedent(
        """
    # Import Symphony MARC Records to FOLIO
    Workflow for monitoring a file mount of exported MARC21 records from
    Symphony ILS into [FOLIO](https://www.folio.org/) LSM.
    """
    )


    monitor_file_mount = FileSensor(
        task_id="marc21_monitor",
        fs_conn_id="bib_path",
        filepath="/opt/airflow/symphony/*.*rc",
        timeout=270,  # 4 1/2 minutes
    )

    monitor_file_mount.doc_md = dedent(
        """\
        ####  Monitor File Mount
        Monitor's `/s/SUL/Dataload/Folio` for new MARC21 export files"""
    )

    with TaskGroup(group_id="move-transform") as move_transform_process:


        move_marc_to_instances = PythonOperator(
            task_id="move-marc-files", 
            python_callable=move_marc_files,
            op_kwargs={ "source": "symphony"}
        )

        symphony_csv_to_tsv = PythonOperator(
            task_id="symphony-csv-to-tsv",
            python_callable=tranform_csv_to_tsv,
            op_kwargs={
                "column_names": [
                    'CATKEY',
                    'CALL_NUMBER_TYPE',
                    'BASE_CALL_NUMBER',
                    'VOLUME_INFO',
                    'BARCODE',
                    'LIBRARY',
                    'HOMELOCATION',
                    'CURRENTLOCATION',
                    'ITEM_TYPE'],
                "column_transforms": [
                    ('CATKEY', lambda x: f"a{x}")  # Adds a prefix to match bib 001
                ],
                "marc_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}""",
                "source": "symphony"
            }
        )

        process_marc_files = PythonOperator(
            task_id="preprocess_marc", 
            python_callable=process_marc,
            op_kwargs={ "marc_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}"""}
        )

        finished_move_transform = DummyOperator(
            task_id="finished-move-transform"
        )

        move_marc_to_instances >> process_marc_files >> finished_move_transform
        move_marc_to_instances >> symphony_csv_to_tsv >> finished_move_transform



    with TaskGroup(group_id="marc21-and-tsv-to-folio") as marc_to_folio:

        convert_marc_to_folio_instances = PythonOperator(
            task_id="convert_marc_to_folio_instances",
            python_callable=run_bibs_transformer,
            execution_timeout=timedelta(minutes=10),
            op_kwargs={
                "library_config": sul_config,
                "marc_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}"""
            }
        )

        convert_tsv_to_folio_holdings = PythonOperator(
            task_id="convert_tsv_to_folio_holdings",
            python_callable=run_holdings_tranformer,
            op_kwargs={
                "library_config": sul_config,
                "holdings_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}"""
            }
        )

        convert_tsv_to_folio_items = PythonOperator(
            task_id="convert_tsv_to_folio_items",
            python_callable=run_items_transformer,
            op_kwargs={
                "library_config": sul_config,
                "items_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}"""
            }
        )

        convert_instances_valid_json = PythonOperator(
            task_id="instances_to_valid_json",
            python_callable=process_records,
            op_kwargs={
                "prefix": "folio_instances",
                "out_filename": "instances",
                "jobs": int(parallel_posts),
            },
        )

        convert_holdings_valid_json = PythonOperator(
            task_id="holdings_to_valid_json",
            python_callable=process_records,
            op_kwargs={
                "prefix": "folio_holdings",
                "out_filename": "holdings",
                "jobs": int(parallel_posts),
            },
        )

        convert_items_valid_json = PythonOperator(
            task_id="items_to_valid_json",
            python_callable=process_records,
            op_kwargs={
                "prefix": "folio_items",
                "out_filename": "items",
                "jobs": int(parallel_posts),
            },
        )

        finish_conversion = DummyOperator(task_id="finished-conversion")

        (
            convert_marc_to_folio_instances
            >> convert_tsv_to_folio_holdings
            >> convert_holdings_valid_json
            >> finish_conversion
        )
        (
            convert_marc_to_folio_instances
            >> convert_instances_valid_json
            >> finish_conversion
        )
        (   convert_tsv_to_folio_holdings
            >> convert_tsv_to_folio_items
            >> convert_items_valid_json
            >> finish_conversion
        )

    with TaskGroup(group_id="post-to-folio") as post_to_folio:

        login = PythonOperator(task_id="folio_login",
                               python_callable=folio_login)

        finish_instances = DummyOperator(task_id="finish-posting-instances")

        for i in range(int(parallel_posts)):
            post_instances = PythonOperator(
                task_id=f"post_to_folio_instances_{i}",
                python_callable=post_folio_instance_records,
                op_kwargs={"job": i},
            )

            login >> post_instances >> finish_instances

        finish_holdings = DummyOperator(task_id="finish-posting-holdings")

        for i in range(int(parallel_posts)):
            post_holdings = PythonOperator(
                task_id=f"post_to_folio_holdings_{i}",
                python_callable=post_folio_holding_records,
                op_kwargs={"job": i},
            )

            finish_instances >> post_holdings >> finish_holdings

        finish_items = DummyOperator(task_id="finish-posting-items")

        for i in range(int(parallel_posts)):
            post_items = PythonOperator(
                task_id=f"post_to_folio_items_{i}",
                python_callable=post_folio_items_records,
                op_kwargs={"job": i},
            )

            finish_holdings >> post_items >> finish_items


    archive_instances_holdings_items = PythonOperator(
        task_id="archive_coverted_files",
        python_callable=archive_artifacts
    )


    finish_loading = DummyOperator(
        task_id="finish_loading",
    )

    monitor_file_mount >> move_transform_process >> marc_to_folio
    marc_to_folio >> post_to_folio
    post_to_folio >>  archive_instances_holdings_items >> finish_loading
