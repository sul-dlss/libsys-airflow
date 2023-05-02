import logging
import os
import shutil

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
def archive_task(downloaded_files: list[str], download_path: str, archive_path: str):
    archive(downloaded_files, download_path, archive_path)


def archive(downloaded_files: list[str], download_path: str, archive_path: str):
    if len(downloaded_files) == 0:
        logger.info("No files to archive")
        return

    for filename in downloaded_files:
        download_filepath = _filepath(download_path, filename)
        archive_filepath = _filepath(archive_path, filename)
        shutil.copyfile(download_filepath, archive_filepath)
        logger.info(f"Archived {filename} to {archive_filepath}")


def _filepath(path: str, filename: str) -> str:
    return os.path.join(path, filename)
