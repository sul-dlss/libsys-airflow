from datetime import datetime, timedelta
import logging
import pathlib
import shutil

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy import select
from sqlalchemy.orm import Session

from libsys_airflow.plugins.vendor.paths import archive_basepath
from libsys_airflow.plugins.vendor.models import (
    FileStatus,
    VendorFile,
    VendorInterface
)

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
    set_purge_status(vendor_interfaces)


def _extract_uuids(directory: str):
    """
    Extracts Vendor, VendorInterface, and Filename from Archive Directory
    """
    dir_path = pathlib.Path(directory)
    output = {}
    for vendor_path in dir_path.iterdir():
        for interface_path in vendor_path.iterdir():
            output[interface_path.stem] = {
                "date": dir_path.stem,
                "files": []
            }
            for file in interface_path.iterdir():
                output[interface_path.stem]["files"].append(file.name)
    return output


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


def _process_vendor_file(vendor_file_info: dict, session: Session):
    """
    Iterates through a dictionary of vendors and interfaces and
    updates matched VendorFile records
    """
    for interface_uuid, info in vendor_file_info.items():
        vendor_interface = session.scalars(
            select(VendorInterface).where(
                VendorInterface.folio_interface_uuid == interface_uuid
            )
        ).first()
        updated_date = datetime.strptime(info["date"], "%Y%m%d")
        for filename in info["files"]:
            vendor_file = session.scalars(
                select(VendorFile).where(
                    VendorFile.vendor_interface_id == vendor_interface.id
                )
                .where(VendorFile.vendor_filename == filename)
                .where(VendorFile.expected_execution == updated_date.date())
            ).first()
            vendor_file.status = FileStatus.purged
            vendor_file.updated = datetime.now()
            session.commit()


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


def set_purge_status(vendor_files: list[dict]):
    """
    Finds matching VendorFile from the database and sets status to
    purge
    """
    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        for row in vendor_files:
            _process_vendor_file(row, session)
