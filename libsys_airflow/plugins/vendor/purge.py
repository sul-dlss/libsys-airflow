from datetime import datetime, timedelta
import logging
import pathlib
import shutil

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
    return remove_archived(target_directories)


@task
def set_status_task(vendor_interfaces: list):
    """
    Sets purge status for Files
    """


def find_directories(archive_directory: str, prior_days=90) -> list:
    """
    Iterates through archives to determine what vendor management
    directories to delete based on age
    """
    target_dirs = []
    today = datetime.utcnow()
    prior_90_days = (today - timedelta(days=prior_days)).strftime("%Y%m%d")
    archive_directory = pathlib.Path(archive_directory)
    for directory in sorted(archive_directory.iterdir()):
        if directory.stem <= prior_90_days:
            target_dirs.append(directory)
    if len(target_dirs) < 1:
        logger.info("No directories available for purging")
    return target_dirs


def remove_archived(archived_directories: list[str]) -> list[dict]:
    """
    Removes directories of archived vendor loads and returns a list of
    Vendor/interface uuids for later updates
    """
    vendor_interfaces = []
    for directory in archived_directories:
        vendor_interfaces.append(_extract_uuids(directory))
        shutil.rmtree(directory)
    return vendor_interfaces


def _extract_uuids(directory: str):
    """
    Extracts Vendor, VendorInterface, and Filename from Archive Directory
    """
    dir_path = pathlib.Path(directory)
    output = {}
    for vendor_path in dir_path.iterdir():
        output[vendor_path.stem] = {
            "date": dir_path.stem,
            "interfaces": {}
        }
        for interface_path in vendor_path.iterdir():
            for file in interface_path.iterdir():
                output[vendor_path.stem]["interfaces"][interface_path.stem] = file.name
    return output
