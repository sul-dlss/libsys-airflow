import logging
import ftplib  # noqa
import pathlib
import re

from typing import Union, Optional
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.engine import Engine

from airflow.sdk import task, Variable
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from libsys_airflow.plugins.vendor.models import VendorFile, VendorInterface


logger = logging.getLogger(__name__)

# Interfaces of FTPHook and SFTPHook are similar, but not identical.
# Using adapters to make them compatible.


class FTPAdapter:
    def __init__(self, hook: FTPHook, remote_path: str):
        self.hook = hook
        self.remote_path = remote_path
        self._filenames = hook.list_directory(remote_path)

    def list_directory(self) -> list[str]:
        return self._filenames

    def get_mod_time(self, filename: str) -> str:
        try:
            mod_time = self.hook.get_mod_time(filename)
        except ftplib.error_perm as e:
            logger.warning(f"Failed to retrieve modified time for {filename}, {e}.")
            logger.info(f"Getting modified time for {self.remote_path}/{filename}")
            mod_time = self.hook.get_mod_time(f"{self.remote_path}/{filename}")
        return mod_time.isoformat()

    def get_size(self, filename: str) -> int:
        try:
            self.hook.conn.sendcmd("TYPE I")  # type: ignore
            file_size = self.hook.get_size(filename)
        except ftplib.error_perm as e:
            logger.warning(f"Failed to retrieve size for {filename}, {e}")
            logger.info(f"Getting size for {self.remote_path}/{filename}")
            self.hook.conn.sendcmd("TYPE I")  # type: ignore
            file_size = self.hook.get_size(f"{self.remote_path}/{filename}")

        if file_size is None:
            file_size = 0

        return file_size

    def retrieve_file(self, filename: str, download_filepath: str):
        try:
            self.hook.retrieve_file(filename, download_filepath)
        except ftplib.error_perm as e:
            logger.warning(f"Failed to retrieve {filename}, {e}")
            logger.info(f"Retrieving file {self.remote_path}/{filename}")
            self.hook.retrieve_file(f"{self.remote_path}/{filename}", download_filepath)


class SFTPAdapter:
    def __init__(self, hook: SFTPHook, remote_path: str):
        self.hook = hook
        self.remote_path = remote_path
        self._file_descriptions = hook.describe_directory(remote_path)

    def list_directory(self) -> list[str]:
        return list(self._file_descriptions.keys())

    def get_mod_time(self, filename: str) -> str:
        mod_time_str = self._file_descriptions[filename]["modify"]
        return datetime.strptime(mod_time_str, "%Y%m%d%H%M%S").isoformat()

    def get_size(self, filename: str) -> int | str:
        file_size = self._file_descriptions[filename]["size"]
        if file_size is None:
            file_size = 0

        return file_size

    def retrieve_file(self, filename: str, download_filepath: str):
        remote_filepath = str(pathlib.Path(self.remote_path) / filename)
        self.hook.retrieve_file(remote_filepath, download_filepath)


def create_hook(conn_id: str) -> Union[FTPHook, SFTPHook]:
    """Returns an FTPHook or SFTPHook for the given connection id."""
    hook: Optional[Union[FTPHook, SFTPHook]] = None
    if conn_id.startswith("sftp-"):
        hook = SFTPHook(ftp_conn_id=conn_id)
    else:
        hook = FTPHook(ftp_conn_id=conn_id)
    [success, msg] = hook.test_connection()
    if success:
        return hook
    else:
        logger.error(f"Connection test result is {success}: {msg}")
        raise Exception(msg)


@task
def filter_by_strategy(
    conn_id: str,
    remote_path: str,
    filename_regex: str,
) -> dict:
    """
    Returns a dict of all files from vendor site and files from vendor per filter strategy
    {
        "all_files": [],
        "filtered_files": []
    }
    """
    hook = create_hook(conn_id)
    adapter = _create_adapter(hook, remote_path)
    result: dict = {"all_files": [], "filtered_files": []}
    all_filenames = adapter.list_directory()
    result["all_files"] = all_filenames
    if filename_regex == "CNT-ORD":
        logger.info("Filtered filenames by gobi order strategy")
        result["filtered_files"] = _gobi_order_filter_strategy(all_filenames)
    elif filename_regex:
        logger.info("Filtered filenames by regex strategy")
        result["filtered_files"] = _regex_filter_strategy(
            filename_regex, remote_path, all_filenames
        )
    else:
        logger.info("Filenames not filtered")
        result["filtered_files"] = all_filenames

    return result


@task
def filter_already_downloaded(
    remote_path: str, vendor_uuid: str, vendor_interface_uuid: str, file_list: list
) -> list:
    """
    Checks files from vendor site and VendorFile table to see if already downloaded
    Returns a list of files not yet downloaded
    """
    engine = PostgresHook("vendor_loads").get_sqlalchemy_engine()
    split_files_by_download_status = _filter_already_downloaded(
        file_list, remote_path, vendor_uuid, vendor_interface_uuid, engine
    )
    logger.info(
        f"Already downloaded {len(split_files_by_download_status['already_downloaded'])} files"
    )
    not_yet_downloaded = split_files_by_download_status["not_yet_downloaded"]
    logger.info(f"Filenames not downloaded yet: {not_yet_downloaded}")
    return not_yet_downloaded


@task
def filter_by_mod_date(
    conn_id: str,
    remote_path: str,
    file_list: list,
) -> dict:
    """
    Returns a dict of two lists: files inside download window (default 10 days ago)
    and a list of files to be skipped {"skipped": [], "filtered_files": []}
    """
    result: dict = {"skipped": [], "filtered_files": []}
    download_days_ago = Variable.get("download_days_ago", 10)
    mod_date_after = datetime.now(timezone.utc)
    if download_days_ago:
        mod_date_after = mod_date_after - timedelta(days=int(download_days_ago))

    logger.info(
        f"Filtering files modified after {mod_date_after.isoformat(timespec='seconds')}"
    )

    hook = create_hook(conn_id)
    adapter = _create_adapter(hook, remote_path)

    for filename in file_list:
        # can't compare offset-naive and offset-aware datetimes; make all timezone-naive just in case
        mod_time = datetime.fromisoformat((adapter.get_mod_time(filename))).replace(
            tzinfo=None
        )
        mod_date_after = mod_date_after.replace(tzinfo=None)
        if mod_time > mod_date_after:
            result["filtered_files"].append(filename)
        else:
            file_size = adapter.get_size(filename)
            result["skipped"].append((filename, file_size, mod_time.isoformat()))

    return result


@task(max_active_tis_per_dag=Variable.get("max_active_download_tis", default_var=2))
def download_task(
    conn_id: str,
    remote_path: str,
    download_path: str,
    vendor_interface_name: str,
    files_to_download_list: list,
) -> dict:
    """
    Downloads files from FTP/SFTP and returns a dict of files downloaded, or not
    {
        "fetched": [("filename", "filesize", "datetimeisoformat"), (), ...],
        "fetching_error": [("filename", "filesize", "datetimeisoformat"), (), ...],
        "empty_file_error": [("filename", "filesize", "datetimeisoformat"), (), ...]
    }
    """
    file_statuses: dict = {"fetched": [], "fetching_error": [], "empty_file_error": []}
    hook = create_hook(conn_id)
    adapter = _create_adapter(hook, remote_path)
    logger.info(
        f"Downloading for interface {vendor_interface_name} from {remote_path} with {conn_id}"
    )

    for filename in files_to_download_list:
        download_filepath = (
            f"{download_path}/{_filter_remote_path(filename, remote_path)}"
        )
        mod_time = adapter.get_mod_time(filename)
        file_size = adapter.get_size(filename)
        try:
            logger.info(f"Downloading {filename} ({mod_time}) to {download_filepath}")
            adapter.retrieve_file(filename, download_filepath)
        except Exception:
            logger.error(f"Failed to download {filename} to {download_filepath}")
            file_statuses["fetching_error"].append((filename, file_size, mod_time))
            raise
        else:
            filename = _filter_remote_path(filename, remote_path)
            if int(file_size) < 1:
                logger.error(f"File {filename} size is {file_size}")
                file_statuses["empty_file_error"].append(
                    (filename, file_size, mod_time)
                )
                continue
            file_statuses["fetched"].append((filename, file_size, mod_time))
    return file_statuses


@task
def update_vendor_files_table(
    vendor_files_entries: dict, vendor_uuid: str, vendor_interface_uuid: str
):
    """
    Updates VendorFiles table with info about files downloaded, errored, skipped, etc.
    Input:
    {
        "fetched": [("filename", "filesize", "datetimeisoformat"), (), ...],
        "fetching_error": [("filename", "filesize", "datetimeisoformat"), (), ...],
        "empty_file_error": [("filename", "filesize", "datetimeisoformat"), (), ...]
        "skipped": [("filename", "filesize", "datetimeisoformat"), (), ...]
    }
    """
    engine = PostgresHook("vendor_loads").get_sqlalchemy_engine()
    for status, file_list in vendor_files_entries.items():
        for _ in file_list:
            (filename, file_size, mod_time) = _
            _record_vendor_file(
                filename,
                file_size,
                status,
                vendor_uuid,
                vendor_interface_uuid,
                datetime.fromisoformat(mod_time),
                engine,
            )


def _filter_remote_path(filename: str, remote_path: str) -> str:
    if filename.startswith(remote_path):
        filename = filename.replace(f"{remote_path}/", "")
    return filename


def _record_vendor_file(
    filename: str,
    filesize: int | str | None,
    status: str,
    vendor_uuid: str,
    vendor_interface_uuid: str,
    vendor_timestamp: datetime,
    engine: Engine,
):
    logger.info(
        f"Adding to VendorFile status: {status}, filename: {filename}, file size: {filesize}, vendor uuid: {vendor_uuid}"
    )
    with Session(engine) as session:
        vendor_interface = VendorInterface.load_with_vendor(
            vendor_uuid, vendor_interface_uuid, session
        )
        existing_vendor_file = VendorFile.load_with_vendor_interface(
            vendor_interface, filename, session
        )

        if existing_vendor_file:
            session.delete(existing_vendor_file)

        expected_processing_time = datetime.now(timezone.utc)
        if vendor_interface.processing_delay_in_days:
            expected_processing_time += timedelta(
                days=vendor_interface.processing_delay_in_days
            )

        new_vendor_file = VendorFile(
            created=datetime.now(timezone.utc),
            updated=datetime.now(timezone.utc),
            vendor_interface_id=vendor_interface.id,
            vendor_filename=filename,
            filesize=filesize,
            status=status,
            vendor_timestamp=vendor_timestamp,
            expected_processing_time=expected_processing_time,
        )
        session.add(new_vendor_file)
        session.commit()


def _regex_filter_strategy(
    filename_regex: str, remote_path: str, all_filenames: list[str]
) -> list[str]:
    base_regex = re.compile(filename_regex, flags=re.IGNORECASE)
    remote_path_regex = re.compile(
        filename_regex.replace("^", f"^{remote_path}/"), flags=re.IGNORECASE
    )

    filtered_filenames = []
    for filename in all_filenames:
        if base_regex.match(filename) or remote_path_regex.match(filename):
            filtered_filenames.append(filename)
    return filtered_filenames


def _gobi_order_filter_strategy(all_filenames: list[str]) -> list[str]:
    cnt_basefilenames = [
        pathlib.Path(f).stem for f in all_filenames if f.endswith(".cnt")
    ]
    ord_basefilenames = [
        pathlib.Path(f).stem for f in all_filenames if f.endswith(".ord")
    ]
    basefilenames = list(set(cnt_basefilenames) & set(ord_basefilenames))
    return [f"{f}.ord" for f in basefilenames]


def _create_adapter(
    hook: Union[FTPHook, SFTPHook], remote_path: str
) -> Union[FTPAdapter, SFTPAdapter]:
    if isinstance(hook, SFTPHook):
        return SFTPAdapter(hook, remote_path)
    return FTPAdapter(hook, remote_path)


def _vendor_interface_id(
    vendor_uuid: str, vendor_interface_uuid: str, engine: Engine
) -> int:
    with Session(engine) as session:
        vendor_interface = VendorInterface.load_with_vendor(
            vendor_uuid, vendor_interface_uuid, session
        )
        return vendor_interface.id


def _is_fetched(
    filename: str, remote_path: str, vendor_interface_id: int, engine: Engine
) -> bool:
    filename = _filter_remote_path(filename, remote_path)
    with Session(engine) as session:
        return session.query(
            select(VendorFile)
            .where(VendorFile.vendor_filename == filename)
            .where(VendorFile.vendor_interface_id == vendor_interface_id)
            .where(VendorFile.status.not_in(("not_fetched", "fetching_error")))
            .exists()
        ).scalar()


def _filter_already_downloaded(
    filenames: list[str],
    remote_path: str,
    vendor_uuid: str,
    vendor_interface_uuid: str,
    engine: Engine,
) -> dict:
    """
    Returns a dict of filenames already downloaded and not yet downloaded
    {
        "already_downloaded": [],
        "not_yet_downloaded": []
    }
    """
    vendor_interface_id = _vendor_interface_id(
        vendor_uuid, vendor_interface_uuid, engine
    )
    already_downloaded: list = []
    not_yet_downloaded: list = []
    for f in filenames:
        if _is_fetched(f, remote_path, vendor_interface_id, engine):
            already_downloaded.append(f)
        else:
            not_yet_downloaded.append(f)

    return {
        "already_downloaded": already_downloaded,
        "not_yet_downloaded": not_yet_downloaded,
    }
