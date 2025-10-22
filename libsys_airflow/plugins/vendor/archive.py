import logging
import shutil

from datetime import date
from pathlib import Path

from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy.orm import Session

from libsys_airflow.plugins.vendor.models import VendorFile, VendorInterface
from libsys_airflow.plugins.vendor.paths import archive_path as get_archive_path

logger = logging.getLogger(__name__)


@task
def archive_task(
    downloaded_files: list[str],
    download_path: str,
    vendor_uuid: str,
    vendor_interface_uuid: str,
):
    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        archive(
            downloaded_files,
            Path(download_path),
            vendor_uuid,
            vendor_interface_uuid,
            session,
        )


def archive(
    downloaded_files: list[str],
    download_path: Path,
    vendor_uuid: str,
    vendor_interface_uuid: str,
    session: Session,
):
    if len(downloaded_files) == 0:
        logger.info("No files to archive")
        return

    vendor_interface = VendorInterface.load_with_vendor(
        vendor_uuid, vendor_interface_uuid, session
    )
    for filename in downloaded_files:
        vendor_file = VendorFile.load_with_vendor_interface(
            vendor_interface, filename, session
        )
        archive_file(download_path, vendor_file, session)


def archive_file(
    download_path: Path,
    vendor_file: VendorFile,
    session: Session,
):
    download_filepath = download_path / vendor_file.vendor_filename
    archive_path = get_archive_path(
        vendor_file.vendor_interface.vendor.folio_organization_uuid,
        vendor_file.vendor_interface.interface_uuid,
        date.today(),
    )
    archive_filepath = archive_path / vendor_file.vendor_filename
    archive_path.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(download_filepath, archive_filepath)
    vendor_file.archive_date = date.today()
    session.commit()
    logger.info(f"Archived {vendor_file.vendor_filename} to {archive_filepath}")
