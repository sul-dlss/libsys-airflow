import logging

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

from folio_migration_tools.library_configuration import LibraryConfiguration

from plugins.folio.helpers.marc import post_marc_to_srs

logger = logging.getLogger(__name__)


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

    @task
    def ingestion_marc():
        """
        ### Ingests
        """
        context = get_current_context()
        params = context.get("params")
        srs_filenames = params.get("srs_filenames")
        iteration_id = params.get("iteration_id")

        okapi_username = Variable.get("FOLIO_USER")

        okapi_password = context.get("okapi_password", Variable.get("FOLIO_PASSWORD"))
        logger.info(f"Okapi username: {okapi_username}")

        sul_config = LibraryConfiguration(
            okapi_url=Variable.get("OKAPI_URL"),
            tenant_id="sul",
            okapi_username=okapi_username,
            okapi_password=okapi_password,
            library_name="Stanford University Libraries",
            base_folder="/opt/airflow/migration",
            log_level_debug=True,
            folio_release="lotus",
            iteration_identifier=iteration_id,
        )

        posted_srs_files = []
        for srs_file in srs_filenames:
            logger.info(f"Starting ingestion of {srs_file}")
            post_marc_to_srs(
                dag_run=context.get("dag_run"),
                library_config=sul_config,
                srs_filename=srs_file,
                iteration_id=iteration_id,
                MAX_ENTITIES=Variable.get("MAX_SRS_ENTITIES", 250),
            )
            posted_srs_files.append(srs_file)

        return posted_srs_files

    @task
    def finish():
        logger.info("Finished migration")

    ingestion_marc() >> finish()


ingest_marc_to_srs = add_marc_to_srs()
