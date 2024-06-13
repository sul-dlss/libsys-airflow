import logging
import pathlib

import pymarc

from libsys_airflow.plugins.data_exports.marc.transformer import Transformer
from libsys_airflow.plugins.data_exports.marc.oclc import OCLCTransformer
from s3path import S3Path

logger = logging.getLogger(__name__)

excluded_tags = [
    '592',
    '594',
    '596',
    '597',
    '598',
    '599',
    '695',
    '699',
    '790',
    '791',
    '792',
    '793',
    '850',
    '851',
    '853',
    '854',
    '855',
    '863',
    '864',
    '866',
    '871',
    '890',
    '891',
    '897',
    '898',
    '899',
    '905',
    '909',
    '911',
    '920',
    '922',
    '925',
    '926',
    '927',
    '928',
    '930',
    '933',
    '934',
    '935',
    '936',
    '937',
    '942',
    '943',
    '944',
    '945',
    '946',
    '947',
    '948',
    '949',
    '950',
    '951',
    '952',
    '954',
    '955',
    '957',
    '959',
    '960',
    '961',
    '962',
    '963',
    '965',
    '966',
    '967',
    '971',
    '975',
    '980',
    '983',
    '984',
    '985',
    '986',
    '987',
    '988',
    '990',
    '996',
]

oclc_excluded = [
    '001',
    '002',
    '003',
    '004',
    '005',
    '029',
    '049',
    '066',
    '099',
    '583',
    '593',
    '595',
    '690',
    '691',
    '693',
    '696',
    '697',
    '698',
    '795',
    '796',
    '799',
    '852',
    '901',
    '910',
    '918',
    '923',
    '924',
    '940',
    '981',
    '993',
    '998',
    '999',
]


def add_holdings_items_to_marc_files(marc_file_list: dict, full_dump: bool):
    transformer = Transformer()
    new_and_updates = marc_file_list['new'] + marc_file_list['updates']
    for marc_file in new_and_updates:
        transformer.add_holdings_items(marc_file=marc_file, full_dump=full_dump)


def divide_into_oclc_libraries(**kwargs):
    marc_list = kwargs.get("marc_file_list", [])

    oclc_transformer = OCLCTransformer()

    for marc_file in marc_list:
        oclc_transformer.divide(marc_file)

    oclc_transformer.save()

    return oclc_transformer.staff_notices


def change_leader_for_deletes(marc_file_list: dict):
    for file in marc_file_list['deletes']:
        leader_for_deletes(file, False)


def leader_for_deletes(marc_file: str, full_dump: bool):
    """
    Records specified as deleted by using d in position 05 in the MARC Leader
    """
    marc_path = pathlib.Path(marc_file)
    if full_dump:
        marc_path = S3Path(marc_file)
        logger.info(f"Changing leader using AWS S3 with path: {marc_path}")

    with marc_path.open('rb') as fo:
        marc_records = [record for record in pymarc.MARCReader(fo)]

    logger.info(f"Changing leader for {len(marc_records):,} records")

    for i, record in enumerate(marc_records):
        try:
            record.leader = pymarc.leader.Leader(record.leader)
            record.leader[5] = "d"  # type: ignore
            if not i % 100:
                logger.info(f"{i:,} records processed")
        except AttributeError as e:
            logger.warning(e)
            continue

    try:
        with marc_path.open("wb") as fo:
            marc_writer = pymarc.MARCWriter(fo)
            for record in marc_records:
                marc_writer.write(record)
    except pymarc.exceptions.WriteNeedsRecord as e:
        logger.warning(e)


def remove_fields_from_marc_files(marc_file_list: dict):
    for file in marc_file_list['new']:
        remove_marc_fields(file, False)
    logger.info(f"Removed MARC fields from these New files {marc_file_list['new']}")

    for file in marc_file_list['updates']:
        remove_marc_fields(file, False)
    logger.info(
        f"Remove MARC fields from these Updated files {marc_file_list['updates']}"
    )


def remove_marc_fields(marc_file: str, full_dump: bool):
    """
    Removes MARC fields from export MARC21 file
    """
    marc_path = pathlib.Path(marc_file)
    if full_dump:
        marc_path = S3Path(marc_file)
        logger.info(f"Removing MARC fields using AWS S3 with path: {marc_path}")

    with marc_path.open('rb') as fo:
        marc_records = [record for record in pymarc.MARCReader(fo)]

    logger.info(f"Removing MARC fields for {len(marc_records):,} records")

    for i, record in enumerate(marc_records):
        try:
            record.remove_fields(*excluded_tags)
            if not i % 100:
                logger.info(f"{i:,} records processed")
        except AttributeError as e:
            logger.warning(e)
            continue

    try:
        with marc_path.open("wb") as fo:
            marc_writer = pymarc.MARCWriter(fo)
            for record in marc_records:
                marc_writer.write(record)
    except pymarc.exceptions.WriteNeedsRecord as e:
        logger.warning(e)


def remove_marc_files(marc_file_list: list):
    for file_path_str in marc_file_list:
        file_path = pathlib.Path(file_path_str)
        file_path.unlink()
        logger.info(f"Removed {file_path}")
