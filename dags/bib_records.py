"""Imports exported MARC records from Symphony into FOLIO"""

from datetime import datetime, timedelta
import logging

from textwrap import dedent
from typing_extensions import TypeAlias  # noqa

from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from folio_migration_tools.library_configuration import LibraryConfiguration

from plugins.folio.helpers import (
    move_marc_files_check_tsv,
    process_marc,
    process_records,
    transform_move_tsvs,
)
from plugins.folio.holdings import electronic_holdings, run_holdings_tranformer, post_folio_holding_records

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
    folio_release="lotus",
    iteration_identifier="",
)

max_entities = Variable.get("MAX_ENTITIES", 500)
parallel_posts = Variable.get("parallel_posts", 3)

default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Determines marc_only workflow
def marc_only(*args, **kwargs):
    task_instance = kwargs["task_instance"]

    all_next_task_id = kwargs.get("default_task")
    marc_only_task_id = kwargs.get("marc_only_task")

    marc_only_workflow = task_instance.xcom_pull(
        key="marc_only", task_ids="move-transform.move-marc-files"
    )

    if marc_only_workflow:
        return marc_only_task_id
    return all_next_task_id


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
            python_callable=move_marc_files_check_tsv,
            op_kwargs={"source": "symphony"},
        )

        marc_only_transform = BranchPythonOperator(
            task_id="marc-only-transform-check",
            python_callable=marc_only,
            op_kwargs={
                "marc_stem": "{{ ti.xcom_pull('move-transform.move-marc-files') }}",  # noqa
                "default_task": "move-transform.symphony-tsv-processing",
                "marc_only_task": "move-transform.finished-move-transform",
            },
        )

        symphony_tsv_processing = PythonOperator(
            task_id="symphony-tsv-processing",
            python_callable=transform_move_tsvs,
            op_kwargs={
                "column_transforms": [
                    # Adds a prefix to match bib 001
                    ("CATKEY", lambda x: x if x.startswith("a") else f"a{x}"),
                    # Strips out spaces from barcode
                    ("BARCODE", lambda x: x.strip()),
                ],
                "tsv_stem": "{{ ti.xcom_pull('move-transform.move-marc-files') }}",  # noqa
                "source": "symphony",
            },
        )

        process_marc_files = PythonOperator(
            task_id="preprocess_marc",
            python_callable=process_marc,
            op_kwargs={
                "marc_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}"""  # noqa
            },
        )

        finished_move_transform = DummyOperator(
            task_id="finished-move-transform", trigger_rule="none_failed_or_skipped"
        )

        move_marc_to_instances >> process_marc_files >> marc_only_transform
        marc_only_transform >> symphony_tsv_processing >> finished_move_transform
        marc_only_transform >> finished_move_transform

    with TaskGroup(group_id="marc21-and-tsv-to-folio") as marc_to_folio:

        convert_marc_to_folio_instances = PythonOperator(
            task_id="convert_marc_to_folio_instances",
            python_callable=run_bibs_transformer,
            execution_timeout=timedelta(minutes=10),
            op_kwargs={
                "library_config": sul_config,
                "marc_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}""",  # noqa
            },
        )

        marc_only_convert_check = BranchPythonOperator(
            task_id="marc-only-convert-check",
            python_callable=marc_only,
            op_kwargs={
                "marc_stem": "{{ ti.xcom_pull('move-transform.move-marc-files') }}",  # noqa
                "default_task": "marc21-and-tsv-to-folio.convert_tsv_to_folio_holdings",  # noqa
                "marc_only_task": "marc21-and-tsv-to-folio.finished-conversion",  # noqa
            },
        )

        convert_tsv_to_folio_holdings = PythonOperator(
            task_id="convert_tsv_to_folio_holdings",
            python_callable=run_holdings_tranformer,
            op_kwargs={
                "library_config": sul_config,
                "holdings_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}""",  # noqa
            },
        )

        generate_electronic_holdings = PythonOperator(
            task_id="generate-electronic-holdings",
            python_callable=electronic_holdings,
            op_kwargs={
                "library_config": sul_config,
                "electronic_holdings_id": "996f93e2-5b5e-4cf2-9168-33ced1f95eed",
                "holdings_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}"""
            },
        )

        convert_tsv_to_folio_items = PythonOperator(
            task_id="convert_tsv_to_folio_items",
            python_callable=run_items_transformer,
            op_kwargs={
                "library_config": sul_config,
                "items_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}""",  # noqa
            },
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

        finish_conversion = DummyOperator(
            task_id="finished-conversion",
            trigger_rule="none_failed_or_skipped",
        )

        convert_marc_to_folio_instances >> marc_only_convert_check
        (
            convert_marc_to_folio_instances
            >> convert_instances_valid_json
            >> finish_conversion
        )
        (convert_marc_to_folio_instances >> finish_conversion)  # noqa
        marc_only_convert_check >> [
            convert_tsv_to_folio_holdings,
            finish_conversion,
        ]  # noqa
        (
            convert_tsv_to_folio_holdings
            >> generate_electronic_holdings
            >> convert_holdings_valid_json
            >> finish_conversion
        )
        (
            convert_tsv_to_folio_holdings
            >> convert_tsv_to_folio_items
            >> convert_items_valid_json
            >> finish_conversion
        )

    with TaskGroup(group_id="post-to-folio") as post_to_folio:

        login = PythonOperator(
            task_id="folio_login", python_callable=folio_login
        )  # noqa

        finish_instances = DummyOperator(task_id="finish-posting-instances")

        finished_all_posts = DummyOperator(
            task_id="finish-all-posts", trigger_rule="none_failed_or_skipped"
        )

        for i in range(int(parallel_posts)):
            post_instances = PythonOperator(
                task_id=f"post_to_folio_instances_{i}",
                python_callable=post_folio_instance_records,
                op_kwargs={"job": i, "MAX_ENTITIES": max_entities},
            )

            login >> post_instances >> finish_instances

        marc_only_post_check = BranchPythonOperator(
            task_id="marc-only-post-check",
            python_callable=marc_only,
            op_kwargs={
                "default_task": "post-to-folio.start-holdings-posting",
                "marc_only_task": "post-to-folio.finish-all-posts",
            },
        )

        start_holdings = DummyOperator(task_id="start-holdings-posting")

        (
            finish_instances
            >> marc_only_post_check
            >> [start_holdings, finished_all_posts]
        )  # noqa

        finish_holdings = DummyOperator(task_id="finish-posting-holdings")

        for i in range(int(parallel_posts)):
            post_holdings = PythonOperator(
                task_id=f"post_to_folio_holdings_{i}",
                python_callable=post_folio_holding_records,
                op_kwargs={"job": i, "MAX_ENTITIES": max_entities},
            )

            start_holdings >> post_holdings >> finish_holdings

        finish_items = DummyOperator(task_id="finish-posting-items")

        for i in range(int(parallel_posts)):
            post_items = PythonOperator(
                task_id=f"post_to_folio_items_{i}",
                python_callable=post_folio_items_records,
                op_kwargs={"job": i, "MAX_ENTITIES": max_entities},
            )

            finish_holdings >> post_items >> finish_items >> finished_all_posts

    ingest_srs_records = TriggerDagRunOperator(
        task_id="ingest-srs-records",
        trigger_dag_id="add_marc_to_srs",
        conf={
            "srs_filename": "folio_srs_instances_{{ dag_run.run_id }}_bibs-transformer.json"
        },
    )

    remediate_errors = TriggerDagRunOperator(
        task_id="remediate-errors",
        trigger_dag_id="fix_failed_record_loads",
        trigger_run_id="{{ dag_run.run_id }}",
    )

    finish_loading = DummyOperator(
        task_id="finish_loading",
    )

    monitor_file_mount >> move_transform_process >> marc_to_folio
    marc_to_folio >> post_to_folio >> finish_loading
    finish_loading >> [ingest_srs_records, remediate_errors]
