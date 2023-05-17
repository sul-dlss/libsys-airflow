import logging
import pathlib

import pymarc
import magic

from airflow.decorators import task
from airflow.models import Variable

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
    filter_fields(pathlib.Path(marc_path), fields)


def filter_fields(marc_path: pathlib.Path, fields: list[str]):
    """
    Filters specified fields from MARC records
    """
    logger.info(f"Filtering fields from {marc_path}")
    filtered_records = []
    with marc_path.open("rb") as fo:
        reader = _marc_reader(fo)
        for record in reader:
            if record is None:
                logger.info(
                    f"Error reading MARC. Current chunk: {reader.current_chunk}. Error: {reader.current_exception} "
                )
            else:
                record.remove_fields(*fields)
                filtered_records.append(record)

    with marc_path.open("wb") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in filtered_records:
            record.force_utf8 = True
            marc_writer.write(record)

    logger.info(f"Finished filtering fields from {marc_path}")


def is_marc(path: pathlib.Path):
    return magic.from_file(str(path), mime=True) == "application/marc"


def _marc_reader(file):
    return pymarc.MARCReader(
        file, to_unicode=True, permissive=True, utf8_handling="replace"
    )


@task
def batch_task(download_path: str, filename: str) -> list[str]:
    """
    Splits a MARC file into batches.
    """
    marc_path = pathlib.Path(download_path) / filename
    if not is_marc(marc_path):
        logger.info(f"Skipping batching {filename}")
        return [filename]
    max_records = Variable.get("MAX_ENTITIES", 500)
    return batch(download_path, filename, max_records)


def batch(download_path: str, filename: str, max_records: int) -> list[str]:
    """
    Splits a MARC file into batches.
    """
    records = []
    index = 1
    batch_filenames = []
    with open(pathlib.Path(download_path) / filename, "rb") as fo:
        reader = _marc_reader(fo)
        for record in reader:
            if record is None:
                logger.info(
                    f"Error reading MARC. Current chunk: {reader.current_chunk}. Error: {reader.current_exception} "
                )
            else:
                records.append(record)
                if len(records) == max_records:
                    records, index = _new_batch(
                        download_path, filename, index, batch_filenames, records
                    )
    if len(records) > 0:
        _new_batch(download_path, filename, index, batch_filenames, records)
    logger.info(f"Finished batching {filename} into {index} files")
    return batch_filenames


def _new_batch(download_path, filename, index, batch_filenames, records):
    batch_filename = _batch_filename(filename, index)
    batch_filenames.append(batch_filename)
    _write_records(records, download_path, batch_filename)
    return [], index + 1


def _batch_filename(filename, index):
    file_path = pathlib.Path(filename)
    return f"{file_path.stem}_{index}{file_path.suffix}"


def _write_records(records, download_path, filename):
    marc_path = pathlib.Path(download_path) / filename
    logger.info(f"Writing {len(records)} records to {marc_path}")
    with marc_path.open("wb") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in records:
            record.force_utf8 = True
            marc_writer.write(record)


def move_fields(marc_path: pathlib.Path, change_fields: list[dict]):
    """
    Moves MARC fields within each record in a file.
    """
    if not all(change.keys() == {"from", "to"} for change in change_fields):
        logger.error(
            f"Changes {change_fields} must all include 'from' and 'to'. Not moving fields."
        )
        return

    logger.info(f"Moving fields in {marc_path}")
    updated_records = []
    with marc_path.open("rb") as fo:
        reader = _marc_reader(fo)
        for record in reader:
            if record is None:
                logger.info(
                    f"Error reading MARC. Current chunk: {reader.current_chunk}. Error: {reader.current_exception} "
                )
            else:
                updated_record = _change_fields(record, change_fields)
                updated_records.append(updated_record)

    with marc_path.open("wb") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in updated_records:
            record.force_utf8 = True
            marc_writer.write(record)

    logger.info(f"Finished moving fields in {marc_path}")


def _change_fields(record, change_fields):
    for change in change_fields:
        for field in record.get_fields(change["from"]):
            field.tag = change["to"]
    return record
