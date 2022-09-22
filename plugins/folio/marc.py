import logging

from pathlib import Path

from folio_migration_tools.migration_tasks.batch_poster import BatchPoster

logger = logging.getLogger(__name__)


def _check_srs_existence(**kwargs) -> list:
    """Checks to see if SRS records exist, returns a list of existing JSON files"""
    airflow = kwargs.get("airflow", "/opt/airflow")
    srs_files = kwargs.get("srs_files")

    results_dir = Path(airflow) / "migration/results"

    existing_srs_files = []

    for srs_file in srs_files:
        srs_file_path = results_dir / srs_file
        if srs_file_path.exists():
            existing_srs_files.append({"file_name": srs_file})

    return existing_srs_files


def post_marc_to_srs(*args, **kwargs):
    dag = kwargs.get("dag_run")

    srs_files = _check_srs_existence(**kwargs)

    task_config = BatchPoster.TaskConfiguration(
        name="marc-to-srs-batch-poster",
        migration_task_type="BatchPoster",
        object_type="SRS",
        files=srs_files,
        batch_size=kwargs.get("MAX_ENTITIES", 1000),
    )

    library_config = kwargs["library_config"]
    library_config.iteration_identifier = dag.run_id

    srs_batch_poster = BatchPoster(task_config, library_config, use_logging=False)

    srs_batch_poster.do_work()

    srs_batch_poster.wrap_up()

    logger.info("Finished posting MARC json to SRS")

    return srs_files


def remove_srs_json(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    srs_filenames = kwargs["srs_filenames"]

    srs_filedir = Path(airflow) / "migration/results/"

    for srs_file in srs_filenames:
        srs_file_path = srs_filedir / srs_file
        if srs_file_path.exists():
            srs_file_path.unlink()
            logger.info(f"Removed {srs_file_path}")
