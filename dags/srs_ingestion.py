import os
import logging


from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator


from filesplit.split import Split
from folio_migration_tools.library_configuration import LibraryConfiguration

from plugins.folio.marc import post_marc_to_srs, remove_srs_json

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


@dag(
    schedule_interval=None,
    start_date=datetime(2022, 6, 23),
    catchup=False,
    tags=["folio", "bib_import"],
    max_active_runs=1,
)
def add_marc_to_srs():
    """
    ## Adds MARC JSON to Source Record Storage
    After a successful symphony_marc_import DAG run, takes the
    folio_srs_instances_{dag-run}_bibs-transformer.json file and attempts to
    batch POSTS to the Okapi endpoint
    """

    get_inventory_indices = PostgresOperator(
        task_id="get-inventory-indices",
        sql="""
        SELECT indexname, indexdef FROM pg_indexes
        WHERE schemaname = 'sul_mod_inventory_storage' AND
        (tablename = 'instance' OR tablename = 'holdings_records'
        OR tablename = 'item')
        """,
        postgres_conn_id="postgres_folio",
        database="okapi",
    )

    @task
    def drop_indexes():
        index_result = get_inventory_indices.output["return_value"]
        sql = ""
        for row in index_result:
            name = row[0]
            if name.endswith("pkey"):
                continue
            sql = f"{sql}DROP INDEX sul_mod_inventory_storage.{name};"
        return PostgresOperator(
            task_id="remove-srs-indices",
            sql=sql,
            postgres_conn_id="postgres_folio",
        )

    @task
    def save_inventory_storage_sql():
        """
        ### Saves mod-inventory-storage indices to file
        """
        index_result = get_inventory_indices.output["return_value"]
        with open("sql/sul_mod_inventory_storage_indexes.sql", "w+") as fo:
            for index in index_result:
                fo.write(f"{index[1]}\n")
            fo.write("VACUUM ANALYZE sul_mod_inventory_storage.instance;\n")
            fo.write("VACUUM ANALYZE sul_mod_inventory_storage.holdings_record;\n")
            fo.write("VACUUM ANALYZE sul_mod_inventory_storage.item;\n")

    @task
    def ingestion_marc():
        """
        ### Ingests SRS record as JSON
        """
        context = get_current_context()
        srs_filename = context.get("params").get("srs_filename")
        srs_dir = os.path.dirname(srs_filename)
        split = Split(srs_filename, srs_dir)
        """
        Splits files are generated in this fashion:
            [original_filename]_1.json, [original_filename]_2.json, .., [original_filename]_n.json
        """
        split.bylinecount(Variable.get("NUMBER_OF_SRS_FILES", 10000))

        for srs_file in Path(srs_dir).glob("*_[0-9].json"):
            logger.info(f"Starting ingestion of {srs_file}")
            post_marc_to_srs(
                dag_run=context.get("dag_run"),
                library_config=sul_config,
                srs_file=str(srs_file),
                MAX_ENTITIES=Variable.get("MAX_SRS_ENTITIES", 500),
            )

    restore_indices = PostgresOperator(
        task_id="restore-inventory-indices",
        sql="sql/sul_mod_inventory_storage_indexes.sql",
        postgres_conn_id="postgres_folio",
    )

    @task
    def cleanup():
        context = get_current_context()
        srs_filename = context.get("params").get("srs_filename")
        logger.info(f"Removing SRS JSON {srs_filename} and split files")
        remove_srs_json(srs_filename=srs_filename)

    @task
    def finish():
        context = get_current_context()
        srs_filename = context.get("params").get("srs_filename")
        logger.info(f"Finished migration {srs_filename}")

    ingestion_marc_task = ingestion_marc()
    (
        get_inventory_indices
        >> [save_inventory_storage_sql(), drop_indexes()]
        >> ingestion_marc_task
        >> restore_indices
        >> cleanup()
        >> finish()
    )


ingest_marc_to_srs = add_marc_to_srs()
