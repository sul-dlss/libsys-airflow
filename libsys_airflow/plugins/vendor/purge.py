from datetime import datetime, timedelta
import logging
import pathlib
import shutil

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy import select
from sqlalchemy.orm import Session

from libsys_airflow.plugins.vendor.paths import archive_basepath, downloads_basepath
from libsys_airflow.plugins.vendor.models import FileStatus, VendorFile
from libsys_airflow.plugins.vendor.vendor_interface import load_vendor_interface

logger = logging.getLogger(__name__)


@task(multiple_outputs=True)
def discover_task() -> dict[list]:
    """
    Task for discovering 90-day old active files and archived directories
    of vendor file loads to be deleted
    """
    return {
        "archive": find_directories(archive_basepath()),
        "downloads": find_files(downloads_basepath()),
    }


@task
def remove_downloads_task(target_files: list) -> list:
    """
    Task takes a list of paths and attempts to delete
    """
    return remove_files(target_files)


@task
def remove_archives_task(target_directories: list) -> list:
    """
    Task takes a list of files and attempts to delete
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
            output[interface_path.stem] = {"date": dir_path.stem, "files": []}
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
            target_dirs.append(str(directory))
    if len(target_dirs) < 1:
        logger.info("No directories available for purging")
    return target_dirs


def find_files(downloads_directory: str, prior_days=90):
    """
    Iterates through downloads directory determing what files to
    delete based on the file's age
    """
    prior_timestamp = (datetime.utcnow() - timedelta(days=prior_days)).timestamp()
    downloads_path = pathlib.Path(downloads_directory)
    files = []
    for file_path in downloads_path.glob("**/*"):
        if file_path.is_file() and file_path.stat().st_mtime <= prior_timestamp:
            logger.info(f"Found {file_path}")
            files.append(str(file_path.absolute()))
    return files


def _process_vendor_file(vendor_file_info: dict, session: Session):
    """
    Iterates through a dictionary of vendors and interfaces and
    updates matched VendorFile records
    """
    for interface_uuid, info in vendor_file_info.items():
        vendor_interface = load_vendor_interface(interface_uuid, session)
        updated_date = datetime.strptime(info["date"], "%Y%m%d")
        for filename in info["files"]:
            vendor_file = session.scalars(
                select(VendorFile)
                .where(VendorFile.vendor_interface_id == vendor_interface.id)
                .where(VendorFile.vendor_filename == filename)
                .where(VendorFile.expected_execution == updated_date.date())
            ).first()
            if vendor_file is None:
                logger.error(f"{filename} for {vendor_interface} not found")
                continue
            vendor_file.status = FileStatus.purged
            vendor_file.updated = datetime.now()
            logger.info(f"Updated {vendor_file}")
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
        logger.info(f"Removed {directory}")
    return vendor_interfaces


def remove_files(target_files: list[str]) -> bool:
    """
    Removes files and logs result
    """
    for file in target_files:
        file_path = pathlib.Path(file)
        if file_path.exists():
            file_path.unlink()
            logger.info(f"Removed {file}")
    return True


def set_purge_status(vendor_files: list[dict]) -> bool:
    """
    Finds matching VendorFile from the database and sets status to
    purge
    """
    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        for row in vendor_files:
            _process_vendor_file(row, session)
    return True
