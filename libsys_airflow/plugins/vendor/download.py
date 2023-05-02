import logging
import re
import pathlib
from typing import Union
import os

from airflow.decorators import task
from airflow.providers.ftp.hooks.ftp import FTPHook, FTPSHook

logger = logging.getLogger(__name__)


@task
def ftp_download_task(
    conn_id: str, remote_path: str, download_path: str, filename_regex: str
) -> list[str]:
    logger.info(f"Connection id is {conn_id}")

    hook = _create_hook(conn_id)
    return download(hook, remote_path, download_path, filename_regex)


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
        hook.retrieve_file(filename, download_filepath)
        logger.info(f"Downloaded {filename} to {download_filepath}")
    return list(filtered_filenames)


def _download_filepath(download_path: str, filename: str) -> str:
    return os.path.join(download_path, filename)
