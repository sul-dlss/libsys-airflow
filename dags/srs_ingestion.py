import os
import logging


from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

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
)
def add_marc_to_srs():
    """
    ## Adds MARC JSON to Source Record Storage
    After a successful symphony_marc_import DAG run, takes the
    folio_srs_instances_{dag-run}_bibs-transformer.json file and attempts to
    batch POSTS to the Okapi endpoint
    """

    @task
    def ingestion_marc():
        """
        ### Ingests
        """
        context = get_current_context()
        srs_filename = context.get("params").get("srs_filename")
        srs_dir = os.path.dirname(srs_filename)
        split = Split(srs_filename, srs_dir)
        """
        Splits files are generated in this fashion:
            [original_filename]_1.ext, [original_filename]_2.ext, .., [original_filename]_n.ext
        """
        split.bylinecount(Variable.get("NUMBER_OF_SRS_FILES", 10000))

        for srs_file in Path(srs_dir).glob("*.ext"):
            logger.info(f"Starting ingestion of {srs_file}")
            post_marc_to_srs(
                dag_run=context.get("dag_run"),
                library_config=sul_config,
                srs_file=str(srs_file),
                MAX_ENTITIES=Variable.get("MAX_SRS_ENTITIES", 500),
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

    ingestion_marc() >> cleanup() >> finish()


ingest_marc_to_srs = add_marc_to_srs()
