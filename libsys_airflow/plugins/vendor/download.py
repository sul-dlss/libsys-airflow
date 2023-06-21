import logging
import re
import pathlib
from typing import Union, Callable, Optional
from datetime import datetime, timedelta

from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.engine import Engine

from airflow.decorators import task
from airflow.models import Variable
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

    def get_mod_time(self, filename: str) -> datetime:
        return self.hook.get_mod_time(filename)

    def get_size(self, filename: str) -> int | None:
        return self.hook.get_size(filename)

    def retrieve_file(self, filename: str, download_filepath: str):
        self.hook.retrieve_file(filename, download_filepath)


class SFTPAdapter:
    def __init__(self, hook: SFTPHook, remote_path: str):
        self.hook = hook
        self.remote_path = remote_path
        self._file_descriptions = hook.describe_directory(remote_path)

    def list_directory(self) -> list[str]:
        return list(self._file_descriptions.keys())

    def get_mod_time(self, filename: str) -> datetime:
        mod_time_str = self._file_descriptions[filename]["modify"]
        # TODO: revisit this type ignore if SFTPHook.describe_directory gets better TypedDict hints, see https://peps.python.org/pep-0589/
        return datetime.strptime(mod_time_str, "%Y%m%d%H%M%S")  # type: ignore

    def get_size(self, filename: str) -> int | str | None:
        return self._file_descriptions[filename]["size"]

    def retrieve_file(self, filename: str, download_filepath: str):
        remote_filepath = str(pathlib.Path(self.remote_path) / filename)
        self.hook.retrieve_file(remote_filepath, download_filepath)


@task(max_active_tis_per_dag=Variable.get("max_active_download_tis", default_var=2))
def ftp_download_task(
    conn_id: str,
    remote_path: str,
    download_path: str,
    filename_regex: str,
    vendor_interface_uuid: str,
) -> list[str]:
    logger.info(
        f"Downloading for interface {vendor_interface_uuid} from {remote_path} with {conn_id}"
    )

    hook = create_hook(conn_id)
    # Note that setting the filename regex to "CNT-ORD" triggers the Gobi order filter strategy.
    if filename_regex == "CNT-ORD":
        filter_strategy = _gobi_order_filter_strategy
    elif filename_regex:
        filter_strategy = _regex_filter_strategy(filename_regex)
    else:
        filter_strategy = _null_filter_strategy()

    download_days_ago = Variable.get("download_days_ago")
    mod_date_after = None
    if download_days_ago:
        mod_date_after = datetime.utcnow() - timedelta(days=int(download_days_ago))

    return download(
        hook,
        remote_path or "",
        download_path,
        filter_strategy,
        vendor_interface_uuid,
        mod_date_after,
    )


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


def download(
    hook: Union[FTPHook, SFTPHook],
    remote_path: str,
    download_path: str,
    filter_strategy: Callable,
    vendor_interface_uuid: str,
    mod_date_after: Optional[datetime],
) -> list[str]:
    """
    Downloads files from FTP/SFTP and returns a list of file paths
    """
    adapter = _create_adapter(hook, remote_path)
    engine = PostgresHook("vendor_loads").get_sqlalchemy_engine()

    all_filenames = adapter.list_directory()
    logger.info(f"All filenames: {all_filenames}")
    filtered_filenames = filter_strategy(all_filenames)
    logger.info(f"Filtered by strategy filenames: {filtered_filenames}")
    filtered_filenames = _filter_already_downloaded(
        filtered_filenames, vendor_interface_uuid, engine
    )
    logger.info(f"Filtered by already downloaded filenames: {filtered_filenames}")
    # Also creates skipped VendorFile for files that are outside download window.
    filtered_filenames = _filter_mod_date(
        filtered_filenames,
        adapter,
        mod_date_after,
        vendor_interface_uuid,
        engine,
    )
    logger.info(f"Filtered by mod filenames: {filtered_filenames}")
    # Airflow does not like long XCOMs so limiting the length,
    filtered_filenames = filtered_filenames[:1000]
    logger.info(f"{len(filtered_filenames)} files to download in {remote_path}")
    for filename in filtered_filenames:
        download_filepath = _download_filepath(download_path, filename)
        mod_time = adapter.get_mod_time(filename)
        try:
            logger.info(f"Downloading {filename} ({mod_time}) to {download_filepath}")
            adapter.retrieve_file(filename, download_filepath)
        except Exception:
            logger.error(f"Failed to download {filename} to {download_filepath}")
            _record_vendor_file(
                filename,
                adapter.get_size(filename),
                "fetching_error",
                vendor_interface_uuid,
                mod_time,
                engine,
            )
            raise
        else:
            _record_vendor_file(
                filename,
                adapter.get_size(filename),
                "fetched",
                vendor_interface_uuid,
                mod_time,
                engine,
            )
    return list(filtered_filenames)


def _download_filepath(download_path: str, filename: str) -> str:
    return str(pathlib.Path(download_path) / filename)


def _record_vendor_file(
    filename: str,
    filesize: int | str | None,
    status: str,
    vendor_interface_uuid: str,
    vendor_timestamp: datetime,
    engine: Engine,
):
    with Session(engine) as session:
        vendor_interface = VendorInterface.load(vendor_interface_uuid, session)
        existing_vendor_file = VendorFile.load_with_vendor_interface(
            vendor_interface, filename, session
        )

        if existing_vendor_file:
            session.delete(existing_vendor_file)

        expected_processing_time = datetime.utcnow()
        if vendor_interface.processing_delay_in_days:
            expected_processing_time += timedelta(
                days=vendor_interface.processing_delay_in_days
            )

        new_vendor_file = VendorFile(
            created=datetime.utcnow(),
            updated=datetime.utcnow(),
            vendor_interface_id=vendor_interface.id,
            vendor_filename=filename,
            filesize=filesize,
            status=status,
            vendor_timestamp=vendor_timestamp,
            expected_processing_time=expected_processing_time,
        )
        session.add(new_vendor_file)
        session.commit()


def _regex_filter_strategy(filename_regex: str) -> Callable:
    def strategy(all_filenames: list[str]) -> list[str]:
        return [
            f
            for f in all_filenames
            if re.compile(filename_regex, flags=re.IGNORECASE).match(f)
        ]

    return strategy


def _null_filter_strategy() -> Callable:
    def strategy(all_filenames: list[str]) -> list[str]:
        return all_filenames

    return strategy


def _gobi_order_filter_strategy() -> Callable:
    def strategy(all_filenames: list[str]) -> list[str]:
        cnt_basefilenames = [
            pathlib.Path(f).stem for f in all_filenames if f.endswith(".cnt")
        ]
        ord_basefilenames = [
            pathlib.Path(f).stem for f in all_filenames if f.endswith(".ord")
        ]
        basefilenames = list(set(cnt_basefilenames) & set(ord_basefilenames))
        return [f"{f}.ord" for f in basefilenames]

    return strategy


def _create_adapter(
    hook: Union[FTPHook, SFTPHook], remote_path: str
) -> Union[FTPAdapter, SFTPAdapter]:
    if isinstance(hook, SFTPHook):
        return SFTPAdapter(hook, remote_path)
    return FTPAdapter(hook, remote_path)


def _vendor_interface_id(vendor_interface_uuid: str, engine: Engine) -> int:
    with Session(engine) as session:
        vendor_interface = VendorInterface.load(vendor_interface_uuid, session)
        return vendor_interface.id


def _is_fetched(filename: str, vendor_interface_id: int, engine: Engine) -> bool:
    with Session(engine) as session:
        return session.query(
            select(VendorFile)
            .where(VendorFile.vendor_filename == filename)
            .where(VendorFile.vendor_interface_id == vendor_interface_id)
            .where(VendorFile.status.not_in(("not_fetched", "fetching_error")))
            .exists()
        ).scalar()


def _filter_already_downloaded(
    filenames: list[str], vendor_interface_uuid: str, engine: Engine
) -> list[str]:
    vendor_interface_id = _vendor_interface_id(vendor_interface_uuid, engine)
    return [f for f in filenames if not _is_fetched(f, vendor_interface_id, engine)]


def _filter_mod_date(
    filenames: list[str],
    adapter: Union[FTPAdapter, SFTPAdapter],
    mod_date_after: Optional[datetime],
    vendor_interface_uuid: str,
    engine: Engine,
) -> list[str]:
    if mod_date_after is None:
        return filenames
    logger.info(f"Filtering files modified after {mod_date_after}")
    filtered_filenames = []
    for filename in filenames:
        mod_time = adapter.get_mod_time(filename)
        if mod_time > mod_date_after:
            filtered_filenames.append(filename)
        else:
            _record_vendor_file(
                filename,
                adapter.get_size(filename),
                "skipped",
                vendor_interface_uuid,
                mod_time,
                engine,
            )
    return filtered_filenames
