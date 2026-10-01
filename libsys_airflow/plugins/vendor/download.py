import logging
import ftplib  # noqa
import os
import pathlib
import re

from typing import Union, Optional
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session
from sqlalchemy import or_, select
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
        self._mlsd_checked = False
        self._file_descriptions: Optional[dict] = None

    def _set_binary_mode(self):
        """Sets the transfer type to binary so downloads aren't mangled."""
        try:
            self.hook.get_conn().sendcmd("TYPE I")
        except Exception as e:
            logger.warning(f"Failed to set binary mode: {e}")

    def _reconnect(self):
        """Drops the current connection so the next command opens a new one."""
        if self.hook.conn is None:
            return
        logger.info("Reconnecting to FTP server")
        try:
            self.hook.close_conn()
        except Exception as e:
            # QUIT can fail on a desynced connection too
            logger.warning(f"Failed to close FTP connection: {e}")
            self.hook.conn.close()
            self.hook.conn = None

    def _mlsd_descriptions(self) -> Optional[dict]:
        """Returns MLSD descriptions of remote_path, or None if MLSD is not available."""
        if not self._mlsd_checked:
            self._mlsd_checked = True
            try:
                self._file_descriptions = self.hook.describe_directory(self.remote_path)
                logger.info(f"Successfully used MLSD for {self.remote_path}")
            except ftplib.error_perm as e:
                logger.warning(f"MLSD not available for {self.remote_path}: {e}")
        return self._file_descriptions

    def list_directory(self) -> list[str]:
        descriptions = self._mlsd_descriptions()
        if descriptions is not None:
            return _files_only(descriptions)
        logger.info("Using fallback method (NLST)")
        return [
            _filter_remote_path(filename, self.remote_path)
            for filename in self.hook.list_directory(self.remote_path)
        ]

    def get_mod_time(self, filename: str) -> Optional[str]:
        descriptions = self._mlsd_descriptions()
        if descriptions is None:
            # Without MLSD, query only the requested file rather than the whole directory
            full_path = f"{self.remote_path}/{filename}"
            for _ in range(2):
                try:
                    mod_time = self.hook.get_mod_time(full_path)
                except ftplib.error_perm as e:
                    logger.warning(
                        f"Failed to get modification time for {full_path}: {e}"
                    )
                    return None
                except (ftplib.error_reply, ValueError) as e:
                    # The reply belonged to an earlier command, so the control
                    # connection is out of sync and every later reply would be too
                    logger.warning(f"Unexpected MDTM reply for {full_path}: {e}")
                    self._reconnect()
                else:
                    return mod_time.replace(microsecond=0).isoformat()
            return None

        facts = descriptions.get(filename)
        if facts is None:
            logger.warning(f"{filename} not found in {self.remote_path}")
            return None
        mod_time_str = facts["modify"]
        # MLSD's "modify" fact optionally includes fractional seconds
        try:
            mod_time = datetime.strptime(mod_time_str, "%Y%m%d%H%M%S.%f")
        except ValueError:
            mod_time = datetime.strptime(mod_time_str, "%Y%m%d%H%M%S")
        return mod_time.replace(microsecond=0).isoformat()

    def is_directory(self, filename: str) -> bool:
        descriptions = self._mlsd_descriptions()
        if descriptions is not None:
            return descriptions.get(filename, {}).get("type") in DIRECTORY_TYPES
        # NLST doesn't distinguish folders from files, so try changing into it
        conn = self.hook.get_conn()
        original = conn.pwd()
        try:
            conn.cwd(f"{self.remote_path}/{filename}")
        except ftplib.error_perm:
            return False
        conn.cwd(original)
        return True

    def retrieve_file(self, filename: str, download_filepath: str):
        self._set_binary_mode()
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
        self._file_descriptions: Optional[dict] = None

    def _descriptions(self) -> dict:
        if self._file_descriptions is None:
            self._file_descriptions = self.hook.describe_directory(self.remote_path)
        return self._file_descriptions

    def list_directory(self) -> list[str]:
        return _files_only(self._descriptions())

    def get_mod_time(self, filename: str) -> Optional[str]:
        facts = self._descriptions().get(filename)
        if facts is None:
            logger.warning(f"{filename} not found in {self.remote_path}")
            return None
        return datetime.strptime(facts["modify"], "%Y%m%d%H%M%S").isoformat()

    def is_directory(self, filename: str) -> bool:
        return self._descriptions().get(filename, {}).get("type") in DIRECTORY_TYPES

    def retrieve_file(self, filename: str, download_filepath: str):
        remote_filepath = str(pathlib.Path(self.remote_path) / filename)
        self.hook.retrieve_file(remote_filepath, download_filepath)


DIRECTORY_TYPES = ("dir", "cdir", "pdir")


def _files_only(descriptions: dict) -> list[str]:
    """Returns the names in a directory description that aren't directories."""
    return [
        name
        for name, facts in descriptions.items()
        if facts.get("type") not in DIRECTORY_TYPES
    ]


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
    Returns a dict of files from vendor per filter strategy
    {
        "filtered_files": []
    }
    """
    hook = create_hook(conn_id)
    adapter = _create_adapter(hook, remote_path)
    result: dict = {"filtered_files": []}
    all_filenames = adapter.list_directory()
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

    logger.info(
        f"Found {len(all_filenames)} files in {remote_path}, {len(result['filtered_files'])} matched the filter strategy"
    )
    if all_filenames and not result["filtered_files"]:
        logger.warning(f"No files matched; first 20 files: {all_filenames[:20]}")

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
    Returns a dict of files inside download window (default 10 days ago), files to be
    skipped, and files whose modification time couldn't be retrieved
    {
        "filtered_files": [("filename", "datetimeisoformat"), (), ...],
        "skipped": [("filename", 0, "datetimeisoformat"), (), ...],
        "fetching_error": [("filename", 0, None), (), ...]
    }
    Skipped and errored files aren't downloaded, so their size is recorded as 0.
    Directories are left out of all three lists.
    """
    result: dict = {"skipped": [], "filtered_files": [], "fetching_error": []}
    mod_date_after = _download_cutoff()

    hook = create_hook(conn_id)
    adapter = _create_adapter(hook, remote_path)

    for filename in file_list:
        mod_time_str = adapter.get_mod_time(filename)
        if mod_time_str is None:
            if adapter.is_directory(filename):
                logger.info(f"Ignoring directory {filename}")
                continue
            result["fetching_error"].append((filename, 0, None))
            continue
        # can't compare offset-naive and offset-aware datetimes; make all timezone-naive just in case
        mod_time = datetime.fromisoformat(mod_time_str).replace(tzinfo=None)
        if mod_time > mod_date_after:
            result["filtered_files"].append((filename, mod_time.isoformat()))
        else:
            result["skipped"].append((filename, 0, mod_time.isoformat()))

    return result


@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=30))
def download_task(
    conn_id: str,
    remote_path: str,
    download_path: str,
    vendor_interface_name: str,
    files_to_download_list: list,
) -> dict:
    """
    Downloads files from FTP/SFTP and returns a dict of files downloaded, or not.
    files_to_download_list is [("filename", "datetimeisoformat"), ...] from filter_by_mod_date.
    File sizes are taken from the downloaded local files.
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

    for filename, mod_time in files_to_download_list:
        download_filepath = (
            f"{download_path}/{_filter_remote_path(filename, remote_path)}"
        )
        try:
            logger.info(f"Downloading {filename} ({mod_time}) to {download_filepath}")
            adapter.retrieve_file(filename, download_filepath)
        except Exception:
            logger.error(f"Failed to download {filename} to {download_filepath}")
            file_statuses["fetching_error"].append((filename, 0, mod_time))
            raise
        else:
            filename = _filter_remote_path(filename, remote_path)
            file_size = os.path.getsize(download_filepath)
            if file_size < 1:
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
        "fetching_error": [("filename", "filesize", "datetimeisoformat" or None), (), ...],
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
                datetime.fromisoformat(mod_time) if mod_time else None,
                engine,
            )


def _download_cutoff() -> datetime:
    """Returns the naive UTC time before which vendor files are skipped."""
    download_days_ago = Variable.get("download_days_ago", 10)
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
        days=int(download_days_ago or 0)
    )
    logger.info(
        f"Filtering files modified after {cutoff.isoformat(timespec='seconds')} (download_days_ago: {download_days_ago!r})"
    )
    return cutoff


def _filter_remote_path(filename: str, remote_path: str) -> str:
    if filename.startswith(remote_path):
        filename = filename.replace(f"{remote_path}/", "")
    return filename


def _record_vendor_file(
    filename: str,
    filesize: int | None,
    status: str,
    vendor_uuid: str,
    vendor_interface_uuid: str,
    vendor_timestamp: Optional[datetime],
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
        if vendor_interface.processing_delay_in_days:  # type: ignore
            expected_processing_time += timedelta(
                days=vendor_interface.processing_delay_in_days  # type: ignore
            )

        new_vendor_file = VendorFile(
            created=datetime.now(timezone.utc),
            updated=datetime.now(timezone.utc),
            vendor_interface_id=vendor_interface.id,  # type: ignore
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
        return vendor_interface.id  # type: ignore


def _is_fetched(
    filename: str,
    remote_path: str,
    vendor_interface_id: int,
    cutoff: datetime,
    engine: Engine,
) -> bool:
    """
    Skipped files whose recorded timestamp now falls inside the download window
    aren't considered fetched, so widening the window picks them up.
    """
    filename = _filter_remote_path(filename, remote_path)
    with Session(engine) as session:
        return session.query(
            select(VendorFile)
            .where(VendorFile.vendor_filename == filename)
            .where(VendorFile.vendor_interface_id == vendor_interface_id)
            .where(VendorFile.status.not_in(("not_fetched", "fetching_error")))
            .where(
                or_(
                    VendorFile.status != "skipped",
                    VendorFile.vendor_timestamp.is_(None),
                    VendorFile.vendor_timestamp <= cutoff,  # type: ignore
                )
            )
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
    cutoff = _download_cutoff()
    already_downloaded: list = []
    not_yet_downloaded: list = []
    for f in filenames:
        if _is_fetched(f, remote_path, vendor_interface_id, cutoff, engine):
            already_downloaded.append(f)
        else:
            not_yet_downloaded.append(f)

    return {
        "already_downloaded": already_downloaded,
        "not_yet_downloaded": not_yet_downloaded,
    }
