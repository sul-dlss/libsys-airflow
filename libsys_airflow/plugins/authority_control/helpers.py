import logging
import pathlib

logger = logging.getLogger(__name__)


def clean_up(marc_file_path: str, airflow: str = '/opt/airflow'):
    """
    Moves marc file after running folio data import
    """
    marc_file_path = pathlib.Path(marc_file_path)
    archive_dir = pathlib.Path(airflow) / "authorities/archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    archive_file = archive_dir / marc_file_path.name
    marc_file_path.rename(archive_file)

    logger.info(f"Moved {marc_file_path} to archive")
    return True
