import logging
import re
import pathlib
from typing import Union
import os
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import select

from airflow.decorators import task
from airflow.providers.ftp.hooks.ftp import FTPHook, FTPSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.vendor.models import VendorFile, VendorInterface

logger = logging.getLogger(__name__)


@task
def ftp_download_task(
    conn_id: str,
    remote_path: str,
    download_path: str,
    filename_regex: str,
    vendor_interface_uuid: str,
) -> list[str]:
    logger.info(f"Connection id is {conn_id}")

    hook = _create_hook(conn_id)
    return download(
        hook, remote_path, download_path, filename_regex, vendor_interface_uuid
    )


# Currently this only creates an FTPHook, but could be extended for SFTP and FTPS.
def _create_hook(conn_id: str) -> FTPHook:
    """Returns an FTPHook for the given connection id."""
    hook = FTPHook(ftp_conn_id=conn_id)
    [success, msg] = hook.test_connection()
    if success:
        return hook
    else:
        logger.error(f"Connection test result is {success}: {msg}")
        raise Exception(msg)


# In theory this can also be used for SFTP, but will require testing.
def download(
    hook: Union[FTPHook, FTPSHook],
    remote_path: str,
    download_path: str,
    filename_regex: str,
    vendor_interface_uuid: str,
) -> list[str]:
    """
    Downloads files from FTP/FTPS and returns a list of file paths
    """
    all_filenames = hook.list_directory(remote_path)
    logger.info(f"Found {len(all_filenames)} files in {remote_path}")
    filtered_filenames = all_filenames
    if filename_regex:
        filtered_filenames = [
            f for f in all_filenames if re.compile(filename_regex).match(f)
        ]
        logger.info(
            f"Found {len(filtered_filenames)} files in {remote_path} with {filename_regex}"
        )
    # Remove already downloaded files
    filtered_filenames = [
        f
        for f in filtered_filenames
        if not pathlib.Path(_download_filepath(download_path, f)).is_file()
    ]
    for filename in filtered_filenames:
        download_filepath = _download_filepath(download_path, filename)
        filesize = hook.get_size(filename)
        vendor_timestamp = hook.get_mod_time(filename)
        try:
            hook.retrieve_file(filename, download_filepath)
            logger.info(f"Downloaded {filename} to {download_filepath}")
        except Exception:
            logger.error(f"Failed to download {filename} to {download_filepath}")
            _record_vendor_file(
                filename,
                filesize,
                "fetching_error",
                vendor_interface_uuid,
                vendor_timestamp,
            )
            raise
        else:
            _record_vendor_file(
                filename,
                filesize,
                "fetched",
                vendor_interface_uuid,
                vendor_timestamp,
            )
    return list(filtered_filenames)


def _download_filepath(download_path: str, filename: str) -> str:
    return os.path.join(download_path, filename)


def _record_vendor_file(
    filename: str,
    filesize: int,
    status: str,
    vendor_interface_uuid: str,
    vendor_timestamp: datetime,
):
    pg_hook = PostgresHook("vendor_loads")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        vendor_interface = session.scalars(
            select(VendorInterface).where(
                VendorInterface.folio_interface_uuid == vendor_interface_uuid
            )
        ).first()
        existing_vendor_file = session.scalars(
            select(VendorFile)
            .where(VendorFile.vendor_filename == filename)
            .where(VendorFile.vendor_interface_id == vendor_interface.id)
        ).first()
        if existing_vendor_file:
            session.delete(existing_vendor_file)
        new_vendor_file = VendorFile(
            created=datetime.now(),
            updated=datetime.now(),
            vendor_interface_id=vendor_interface.id,
            vendor_filename=filename,
            filesize=filesize,
            status=status,
            vendor_timestamp=vendor_timestamp,
        )
        session.add(new_vendor_file)
        session.commit()
