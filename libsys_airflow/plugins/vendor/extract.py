import tarfile
import pathlib
import logging
import re
from typing import Optional

import magic

from airflow.decorators import task

from libsys_airflow.plugins.vendor.models import FileStatus
from libsys_airflow.plugins.vendor.file_status import record_status_from_context


logger = logging.getLogger(__name__)


def record_processing_error(context):
    record_status_from_context(context, FileStatus.processing_error)


@task(on_failure_callback=record_processing_error)
def extract_task(
    download_path: str, filename: str, archive_regex: Optional[str] = None
) -> str:
    """
    Extracts a single file from a tar archive.

    The tar must contain either a single file or if regex provided, a single matching file.
    """
    filepath = pathlib.Path(download_path) / filename
    if not _is_tar(filepath):
        logger.info(f"Skipping extracting from {filename}")
        return filename

    return extract(filepath, archive_regex)


def extract(filepath: pathlib.Path, archive_regex: Optional[str] = None) -> str:
    with tarfile.open(filepath, "r") as tar:
        filtered_filename = _filter_filenames(tar.getnames(), archive_regex)
        tar.extract(filtered_filename, filepath.parent)

    return filtered_filename


def _is_tar(file: pathlib.Path) -> bool:
    return (
        magic.Magic(uncompress=True, mime=True).from_file(str(file))
        == "application/x-tar"
    )


def _filter_filenames(filenames: list[str], regex: Optional[str]) -> str:
    filtered_filenames = filenames
    if regex:
        filtered_filenames = [
            f for f in filenames if re.compile(regex, flags=re.IGNORECASE).match(f)
        ]
    if len(filtered_filenames) != 1:
        raise Exception(
            f"Expected to extract 1 file, but found {len(filtered_filenames)}"
        )
    return filtered_filenames[0]
