from datetime import datetime, timedelta
import logging
import pathlib

from airflow.decorators import task

from libsys_airflow.plugins.vendor.paths import archive_basepath
from libsys_airflow.plugins.vendor.models import VendorFile

logger = logging.getLogger(__name__)


@task
def discover_task() -> list[str]:
    """
    Task for discovering archived directories of vendor file loads to
    be deleted
    """
    return find_directories(archive_basepath())


@task
def remove_records_task(target_directories: list) -> list:
    """
    Task take a list of paths and attempts to delete
    """


@task
def set_status_task(vendor_interfaces: list):
    """
    Sets purge status for Files
    """


def find_directories(archive_directory: str) -> list:
    """
    Iteratates through archives to determine what vendor management
    directories to be deleted
    """
    target_dirs = []
    today = datetime.utcnow()
    prior_90_days = (today - timedelta(days=90)).strftime("%Y%m%d")
    archive_directory = pathlib.Path(archive_directory)
    for directory in sorted(archive_directory.iterdir()):
        if directory.stem <= prior_90_days:
            target_dirs.append(directory)
    if len(target_dirs) < 1:
        logger.info("No directories available for purging")
    return target_dirs
