import gzip
import logging
import pathlib

import pymarc
import xml.etree.ElementTree as etree  # noqa

from libsys_airflow.plugins.data_exports.marc.excluded_tags import excluded_tags
from libsys_airflow.plugins.data_exports.marc.transformer import Transformer
from libsys_airflow.plugins.data_exports.marc.oclc import OCLCTransformer
from libsys_airflow.plugins.data_exports.marc.xml_chars import remove_invalid_xml_chars
from libsys_airflow.plugins.data_exports.sql_pool import SQLPool
from s3path import S3Path

logger = logging.getLogger(__name__)


"""
Called by the vendor selection DAGs except oclc and the full record selections
"""


def add_holdings_items_to_marc_files(
    marc_file_list: dict, full_dump: bool, as_xml: bool = False
):
    connection_pool = SQLPool(conn_id="postgres_folio").pool()
    _connection = connection_pool.getconn()
    transformer = Transformer(connection=_connection)
    new_and_updates = marc_file_list['new'] + marc_file_list['updates']
    for marc_file in new_and_updates:
        transformer.add_holdings_items(
            marc_file=marc_file, full_dump=full_dump, as_xml=as_xml
        )

    connection_pool.putconn(_connection, close=True)


def divide_into_oclc_libraries(**kwargs):
    marc_list = kwargs.get("marc_file_list", [])

    oclc_transformer = OCLCTransformer()

    for marc_file in marc_list:
        oclc_transformer.divide(marc_file)

    oclc_transformer.save()

    return oclc_transformer.no_holdings, oclc_transformer.staff_notices


def change_leader_for_deletes(marc_file_list: dict, as_xml: bool = False):
    for file in marc_file_list['deletes']:
        leader_for_deletes(file, False, as_xml=as_xml)


def leader_for_deletes(marc_file: str, full_dump: bool, as_xml: bool = False):
    """
    Records specified as deleted by using d in position 05 in the MARC Leader
    """
    marc_path = pathlib.Path(marc_file)
    if full_dump:
        marc_path = S3Path(marc_file)
        logger.info(f"Changing leader using AWS S3 with path: {marc_path}")

    with marc_path.open('rb') as fo:
        if as_xml:
            marc_records = pymarc.parse_xml_to_array(fo)
        else:
            marc_records = [record for record in pymarc.MARCReader(fo)]

    logger.info(f"Changing leader for {len(marc_records):,} records")

    for i, record in enumerate(marc_records):
        try:
            record.leader[5] = "d"  # type: ignore
            if not i % 100:
                logger.info(f"{i:,} records processed")
        except AttributeError as e:
            logger.warning(e)
            continue

    try:
        with marc_path.open("wb") as fo:
            if as_xml:
                marc_writer = pymarc.XMLWriter(fo)
            else:
                marc_writer = pymarc.MARCWriter(fo)  # type: ignore
            for record in marc_records:
                marc_writer.write(record)
            marc_writer.close(close_fh=False)
    except pymarc.exceptions.WriteNeedsRecord as e:
        logger.warning(e)


def clean_and_serialize_marc_files(marc_file_list: dict, as_xml: bool = False):
    for kind in ["new", "updates", "deletes"]:
        for filepath in marc_file_list.get(kind, []):
            marc_clean_serialize(filepath, False, True, as_xml=as_xml)
            logger.info(
                f"Removed MARC fields and serialized records for '{kind}' files: {filepath}"
            )


def marc_clean_serialize(
    marc_file: str, full_dump: bool, exclude_tags: bool, as_xml: bool = False
):
    """
    Removes MARC fields from export MARC21 or MARC-XML file
    full_dump reads and writes the file in S3
    """
    marc_path = pathlib.Path(marc_file)
    if full_dump:
        marc_path = S3Path(marc_file)
        logger.info(f"Removing MARC fields using AWS S3 with path: {marc_path}")

    with marc_path.open('rb') as fo:
        if as_xml:
            marc_records = pymarc.parse_xml_to_array(fo)
        else:
            marc_records = [record for record in pymarc.MARCReader(fo)]

    if exclude_tags:
        logger.info(f"Removing MARC fields for {len(marc_records):,} records")
        for i, record in enumerate(marc_records):
            try:
                record.remove_fields(*excluded_tags)
                if not i % 100:
                    logger.info(f"{i:,} records processed")
            except AttributeError as e:
                logger.warning(e)
                continue

    """
    Writes the records back to the filesystem
    MARC-XML files are only written below
    """
    if not as_xml:
        try:
            with marc_path.open("wb") as fo:
                marc_writer = pymarc.MARCWriter(fo)  # type: ignore
                for record in marc_records:
                    marc_writer.write(record)
                marc_writer.close()

        except pymarc.exceptions.WriteNeedsRecord as e:
            logger.warning(e)

    logger.info(f"Serializing {len(marc_records)} MARC records as xml")
    try:
        xml_path = marc_path.with_suffix(".xml")
        with xml_path.open("wb") as fo:
            xml_writer = pymarc.XMLWriter(fo)
            for record in marc_records:
                try:
                    record = remove_invalid_xml_chars(record)
                    xml_element = pymarc.record_to_xml_node(record, namespace=True)
                    etree.fromstring(etree.tostring(xml_element))  # noqa
                    xml_writer.write(record)

                except AttributeError as e:
                    logger.error(f"Failed to serialize MARC Record {xml_path}: {e}")
                    continue
                except etree.ParseError as e:
                    logger.error(
                        f"Failed to serialize MARC Record {record['001'].value()}: {e} as xml"
                    )
                    continue

            xml_writer.close()
    except pymarc.exceptions.WriteNeedsRecord as e:
        logger.warning(e)


def remove_marc_files(marc_file_list: list):
    for file_path_str in marc_file_list:
        file_path = pathlib.Path(file_path_str)
        file_path.unlink()
        logger.info(f"Removed {file_path}")


def zip_marc_file(marc_file: str, vendor: str, full_dump: bool = False):
    if full_dump:
        marc_path = S3Path(marc_file)
    else:
        marc_path = pathlib.Path(marc_file)  # type: ignore

    """
    Compress POD and all full-dump files
    """
    logger.info(f"Compressing {marc_path} for vendor {vendor}")
    compress_for = ['pod', 'full-dump']
    if not any(str in vendor for str in compress_for):
        return

    try:
        format = marc_path.suffix
        raw_metadata = marc_path.read_bytes()
        gzip_path = marc_path.with_name(f"{marc_path.stem}{format}.gz")
        compressed_metadata = gzip.compress(raw_metadata)
        gzip_path.write_bytes(compressed_metadata)
        logger.info(f"Compressed METADATA records to {gzip_path}")
        marc_path.unlink()
    except Exception as e:
        logger.warn(e)
