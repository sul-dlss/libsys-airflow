import logging

from pathlib import Path

from folio_migration_tools.migration_tasks.batch_poster import BatchPoster

logger = logging.getLogger(__name__)


def post_marc_to_srs(*args, **kwargs):
    dag = kwargs.get("dag_run")
    srs_filepath = kwargs.get("srs_file")

    task_config = BatchPoster.TaskConfiguration(
        name="marc-to-srs-batch-poster",
        migration_task_type="BatchPoster",
        object_type="SRS",
        files=[{"file_name": srs_filepath}],
        batch_size=kwargs.get("MAX_ENTITIES", 1000),
    )

    library_config = kwargs["library_config"]
    library_config.iteration_identifier = dag.run_id

    srs_batch_poster = BatchPoster(task_config, library_config, use_logging=False)

    srs_batch_poster.do_work()

    srs_batch_poster.wrap_up()

    logger.info("Finished posting MARC json to SRS")


def remove_srs_json(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    srs_filename = kwargs["srs_filename"]

    srs_filedir = Path(airflow) / 'migration/results/'
    for p in Path(srs_filedir).glob(f"{srs_filename}*"):
        p.unlink()
        logger.info(f"Removed {p}")
