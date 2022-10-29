"""Imports exported MARC records from Symphony into FOLIO"""

from datetime import datetime, timedelta
import logging

from textwrap import dedent
from typing_extensions import TypeAlias  # noqa

from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from folio_migration_tools.library_configuration import LibraryConfiguration

from plugins.folio.helpers import get_bib_files, process_records, setup_dag_run_folders

from plugins.folio.helpers.marc import process as process_marc
from plugins.folio.helpers.marc import (
    marc_only,
    move_marc_files,
)

from plugins.folio.helpers.tsv import transform_move_tsvs

from plugins.folio.helpers.folio_ids import (
    generate_holdings_identifiers,
    generate_item_identifiers,
)

from plugins.folio.holdings import (
    electronic_holdings,
    run_holdings_tranformer,
    run_mhld_holdings_transformer,
    post_folio_holding_records,
    update_mhlds_uuids,
)

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


with DAG(
    "symphony_marc_import",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2022, 1, 3),
    catchup=False,
    tags=["bib_import"],
    max_active_runs=Variable.get("IMPORT_MAX_RUNS", 3),
) as dag:

    dag.doc_md = dedent(
        """
    # Import Symphony MARC Records to FOLIO
    Workflow takes exported MARC21 records along with TSV files from
    Symphony ILS and generates Instances, Holdings, and Items records that
    imported into [FOLIO](https://www.folio.org/) LSP.
    """
    )

    bib_files_group = PythonOperator(
        task_id="bib-files-group", python_callable=get_bib_files
    )

    setup_migration_folders = PythonOperator(
        task_id="setup-migration-folders", python_callable=setup_dag_run_folders
    )

    with TaskGroup(group_id="move-transform") as move_transform_process:

        move_marc_to_instances = PythonOperator(
            task_id="move-marc-files",
            python_callable=move_marc_files,
            op_kwargs={
                "marc_filepath": "{{ ti.xcom_pull('bib-files-group', key='marc-file') }}"
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
                    ("BARCODE", lambda x: x.strip() if isinstance(x, str) else x),
                ],
                "tsv_files": "{{ ti.xcom_pull('bib-files-group', key='tsv-files') }}",  # noqa
                # Libraries where FORMAT determines ITEM_TYPE
                "libraries": ["HOOVER", "HV-ARCHIVE"],
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

        move_marc_to_instances >> process_marc_files >> symphony_tsv_processing
        symphony_tsv_processing >> finished_move_transform

    with TaskGroup(group_id="marc21-and-tsv-to-folio") as marc_to_folio:

        convert_marc_to_folio_instances = PythonOperator(
            task_id="convert_marc_to_folio_instances",
            python_callable=run_bibs_transformer,
            execution_timeout=timedelta(minutes=10),
            op_kwargs={
                "library_config": sul_config,
                "marc_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}""",  # noqa
                "dates_tsv": "{{ ti.xcom_pull('bib-files-group', key='tsv-dates') }}",
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

        convert_tsv_to_folio_items = PythonOperator(
            task_id="convert_tsv_to_folio_items",
            python_callable=run_items_transformer,
            op_kwargs={
                "library_config": sul_config,
                "items_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}""",  # noqa
            },
        )

        finish_conversion = DummyOperator(
            task_id="finished-conversion",
            trigger_rule="none_failed_or_skipped",
        )

        convert_marc_to_folio_instances >> convert_tsv_to_folio_holdings
        convert_tsv_to_folio_holdings >> convert_tsv_to_folio_items
        convert_tsv_to_folio_items >> finish_conversion

    with TaskGroup(group_id="mhlds-electronic-holdings") as mhlds_electronic_holdings:

        start_additional_holdings = DummyOperator(
            task_id="start-mhlds-electronic-holdings",
            trigger_rule="none_failed_or_skipped",
        )

        convert_mhld_to_folio_holdings = PythonOperator(
            task_id="convert-mhdl-to-folio-holdings",
            python_callable=run_mhld_holdings_transformer,
            op_kwargs={
                "library_config": sul_config,
                "default_task": "marc21-and-tsv-to-folio.convert_tsv_to_folio_items",
                "mhld_convert_task": "marc21-and-tsv-to-folio.mhdl_to_folio_holdings",
            },
        )

        generate_electronic_holdings = PythonOperator(
            task_id="generate-electronic-holdings",
            python_callable=electronic_holdings,
            op_kwargs={
                "library_config": sul_config,
                "electronic_holdings_id": "996f93e2-5b5e-4cf2-9168-33ced1f95eed",
                "holdings_stem": """{{ ti.xcom_pull('move-transform.move-marc-files') }}""",
            },
        )

        finish_additional_holdings = DummyOperator(task_id="finish-additional-holdings")

        (
            start_additional_holdings
            >> [convert_mhld_to_folio_holdings, generate_electronic_holdings]
            >> finish_additional_holdings
        )

    with TaskGroup(group_id="update-hrids-identifiers") as update_hrids:

        update_holdings_hrids = PythonOperator(
            task_id="update-holdings-idents",
            python_callable=generate_holdings_identifiers,
        )

        update_mhlds_srs = PythonOperator(
            task_id="update-mhlds-srs-uuids", python_callable=update_mhlds_uuids
        )

        update_items = PythonOperator(
            task_id="update-items-idents", python_callable=generate_item_identifiers
        )

        finish_hrid_updates = DummyOperator(task_id="finish-hrid-updates")

        update_holdings_hrids >> [update_mhlds_srs, update_items] >> finish_hrid_updates

    with TaskGroup(group_id="records-to-valid-json") as records_valid_json:

        start_records_to_valid_json = DummyOperator(task_id="start-recs-to-valid-json")

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

        finished_json_conversion = DummyOperator(task_id="finished-json-conversion")

        (
            start_records_to_valid_json
            >> [
                convert_instances_valid_json,
                convert_holdings_valid_json,
                convert_items_valid_json,
            ]
            >> finished_json_conversion
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

        ingest_proceding_succeeding = TriggerDagRunOperator(
            task_id="ingest-proceding-succeeding",
            trigger_dag_id="process_preceding_succeeding_titles",
            conf={"iteration_id": "{{ dag_run.run_id }}"},
        )

        finish_instances >> ingest_proceding_succeeding

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
            "srs_filenames": [
                "folio_srs_instances_bibs-transformer.json",
                "folio_srs_holdings_mhld-transformer.json",
            ],
            "iteration_id": "{{ dag_run.run_id }}",
        },
    )

    remediate_errors = TriggerDagRunOperator(
        task_id="audit_fix_record_loads",
        trigger_dag_id="check_fix_failed_record_loads",
        conf={
            "iteration_id": "{{ dag_run.run_id }}"
        }
    )

    finish_loading = DummyOperator(
        task_id="finish_loading",
    )

    (
        bib_files_group
        >> setup_migration_folders
        >> move_transform_process
        >> marc_to_folio
    )
    marc_to_folio >> mhlds_electronic_holdings >> update_hrids
    update_hrids >> records_valid_json >> post_to_folio >> finish_loading
    finish_loading >> [ingest_srs_records, remediate_errors]
