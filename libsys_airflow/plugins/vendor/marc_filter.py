import logging
import pathlib

import pymarc

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
def filter_fields_task(
    download_path: str, filename: str, fields: list[str] = ["905", "920", "986"]
):
    """
    Filters 905, 920, 986 from MARC records
    """
    marc_path = pathlib.Path(download_path) / filename
    if not is_marc(marc_path):
        logger.info(f"Skipping filtering fields from {marc_path}")
        return
    filter_fields(pathlib.Path(marc_path), ["905", "920", "986"])


def filter_fields(marc_path: pathlib.Path, fields: list[str]):
    """
    Filters specified fields from MARC records
    """
    logger.info(f"Filtering fields from {marc_path}")
    filtered_records = []
    with marc_path.open("rb") as fo:
        marc_reader = pymarc.MARCReader(
            fo, to_unicode=True, permissive=True, utf8_handling="replace"
        )
        for record in marc_reader:
            record.remove_fields(*fields)
            filtered_records.append(record)

    with marc_path.open("wb") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in filtered_records:
            record.force_utf8 = True
            marc_writer.write(record)

    logger.info(f"Finished filtering fields from {marc_path}")


def is_marc(path: pathlib.Path):
    return path.suffix == ".mrc" or path.suffix == ".ord"
