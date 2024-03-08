import logging
import pathlib

import pymarc

from libsys_airflow.plugins.data_exports.marc.transformer import Transformer

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


def add_holdings_items_to_marc_files(marc_file_list: list):
    transformer = Transformer()
    for marc_file in marc_file_list:
        transformer.add_holdings_items(marc_file=marc_file)


def remove_marc_fields(marc_file: str):
    """
    Removes MARC fields from export MARC21 file
    """
    marc_path = pathlib.Path(marc_file)

    with marc_path.open('rb') as fo:
        marc_records = [record for record in pymarc.MARCReader(fo)]

    logger.info(f"Removing MARC fields for {len(marc_records):,} records")

    for i, record in enumerate(marc_records):
        record.remove_fields(*excluded_tags)
        if not i % 100:
            logger.info(f"{i:,} records processed")

    with marc_path.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in marc_records:
            marc_writer.write(record)

