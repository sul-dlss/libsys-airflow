import copy
import csv
import logging
import pathlib

import httpx

from pymarc import (
    JSONHandler as marcJson,
    Record as marcRecord,
)

from libsys_airflow.plugins.data_exports.marc.excluded_tags import excluded_tags
from libsys_airflow.plugins.data_exports.marc.marc_io import marc_writer, record_hrid
from libsys_airflow.plugins.shared.folio_client import folio_client
from airflow.sdk import get_current_context, Variable
from s3path import S3Path
from typing import Union

logger = logging.getLogger(__name__)


class Exporter(object):
    def __init__(self):
        self.folio_client = folio_client()
        self.oversized_records: list = []

    def exceeds_marc21_limits(self, marc_record: marcRecord) -> bool:
        """
        MARC21 allows at most 99,999 bytes per record and 9,999 bytes per field;
        larger records corrupt the binary file for every record after them
        """
        encoding = "utf-8" if marc_record.leader[9] == "a" else "iso8859-1"
        if any(len(field.as_marc(encoding)) > 9999 for field in marc_record.fields):
            return True
        return len(marc_record.as_marc()) > 99999

    def skip_oversized(self, marc_record: marcRecord, uuid: str) -> bool:
        """
        Saves records that exceed the MARC21 limits to oversized_records so
        they can be reported at the end of the DAG run
        """
        if not self.exceeds_marc21_limits(marc_record):
            return False
        hrid = record_hrid(marc_record)
        logger.warning(f"Skipping oversized MARC21 record {hrid} {uuid}")
        self.oversized_records.append({"hrid": hrid, "uuid": uuid})
        return True

    def check_035(self, field035s: list) -> bool:
        reject = False
        for field in field035s:
            if any("gls" in sf for sf in field.get_subfields("a")):
                reject = True
        return reject

    def check_008(self, fields008: list) -> bool:
        reject = False
        for field in fields008:
            lang_code = field.value()[35:38]
            if lang_code not in ["eng", "fre"]:
                reject = True
        return reject

    def check_590(self, field590s: list) -> bool:
        reject = False
        for field in field590s:
            if any("MARCit" in sf for sf in field.get_subfields("a")):
                reject = True
        return reject

    def check_915(self, fields915: list) -> bool:
        reject = False
        for field in fields915:
            if any("NO EXPORT" in sf for sf in field.get_subfields("a")) and any(
                "FOR SU ONLY" in sf for sf in field.get_subfields("b")
            ):
                reject = True
        return reject

    def check_915_authority(self, fields915: list) -> bool:
        reject = False
        for field in fields915:
            if any("NO EXPORT" in sf for sf in field.get_subfields("a")) and any(
                "AUTHORITY VENDOR" in sf for sf in field.get_subfields("b")
            ):
                reject = True
        return reject

    def exclude_marc_by_vendor(self, marc_record: marcRecord, vendor: str):
        """
        Filters MARC record by Vendor
        """
        exclude = False
        match vendor:
            case "gobi":
                exclude = any(
                    [
                        self.check_035(marc_record.get_fields("035")),
                        self.check_008(marc_record.get_fields("008")),
                    ]
                )

            case "oclc" | "pod" | "full-dump":
                exclude = any(
                    [
                        self.check_590(marc_record.get_fields("590")),
                        self.check_915(marc_record.get_fields("915")),
                    ]
                )

            case "backstage":
                exclude = any(
                    [
                        self.check_590(marc_record.get_fields("590")),
                        self.check_915_authority(marc_record.get_fields("915")),
                    ]
                )
        return exclude

    def retrieve_marc_for_instances(
        self, instance_file: pathlib.Path, kind: str, as_xml: bool = False
    ) -> tuple:
        """
        Called for each instanceid file in vendor directory.
        For each ID row, writes and returns converted MARC from SRS to file system
        as_xml writes all of the instance file's records to a single MARC-XML file
        MARC21 records that exceed the format's limits are skipped and saved in
        oversized_records
        """
        if not instance_file.exists():
            raise ValueError(
                f"Instance file does not exist for retrieve_marc_for_instances {instance_file}"
            )

        vendor_name = instance_file.parent.parent.parent.name
        marc_directory = instance_file.parent.parent.parent

        marc_file = ""
        marc_records = []
        not_found_srs_records = []
        with instance_file.open() as fo:
            instance_reader = csv.reader(fo)
            for row in instance_reader:
                uuid = row[0]
                try:
                    marc_record = self.marc21(uuid)
                except httpx.HTTPStatusError as exc:
                    if str(exc).startswith("Client error '404"):
                        not_found_srs_records.append(uuid)
                    else:
                        logger.warning(exc)
                    continue
                except Exception as e:
                    logger.warning(e)
                    continue

                if self.exclude_marc_by_vendor(marc_record, vendor_name):
                    logger.info(f"Excluding {vendor_name}")
                    continue

                if as_xml:
                    marc_records.append(marc_record)
                    continue

                if self.skip_oversized(marc_record, uuid):
                    continue

                marc_file = self.write_marc(
                    instance_file, marc_directory, marc_record, kind
                )

        if marc_records:
            marc_file = self.write_marc(
                instance_file, marc_directory, marc_records, kind, as_xml=True
            )

        return marc_file, not_found_srs_records

    def retrieve_marc_for_full_dump(self, marc_filename: str, instance_ids: str) -> str:
        """
        Called for each instanceid file in the full-dump directory
        For each ID row, writes and returns converted MARC from SRS and writes to AWS bucket
        """
        marc_file = ""
        bucket = Variable.get("FOLIO_AWS_BUCKET", "folio-data-export-prod")
        full_dump_files = f"/{bucket}/data-export-files/full-dump"
        vendor = Variable.get("FULL_DUMP_VENDOR", "full-dump")

        marc = []
        instance_uuids = []
        for row in instance_ids:
            marc_json_handler = marcJson()
            try:
                marc_json_handler.elements(row[2])
                marc21 = marc_json_handler.records[0]
            except Exception as e:
                logger.warning(e)
                continue

            if self.exclude_marc_by_vendor(marc21, vendor):
                continue

            marc.append(marc21)
            instance_uuids.append(row[0])

        logger.info(f"Saving {len(marc)} marc records to {marc_filename} in bucket.")
        marc_file = self.write_marc(
            pathlib.Path(marc_filename),
            S3Path(full_dump_files),
            marc,
            ".",
            as_xml=True,
        )

        """
        CC0 also keeps the SRS records as MARC21, before holdings and items are added
        """
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        if params.get("marc_file_dir") == "CC0":
            cc0_marc = []
            for uuid, record in zip(instance_uuids, marc):
                cc0_record = copy.deepcopy(record)
                if params.get("exclude_tags", True):
                    cc0_record.remove_fields(*excluded_tags)
                if self.skip_oversized(cc0_record, uuid):
                    continue
                cc0_marc.append(cc0_record)
            logger.info(f"Saving {len(cc0_marc)} CC0 MARC21 records")
            self.write_marc(
                pathlib.Path(marc_filename), S3Path(full_dump_files), cc0_marc, "."
            )

        return marc_file

    def marc21(self, instance_uuid: str) -> marcRecord:
        marc_json_handler = marcJson()

        marc_json_handler.elements(self.marc_json_from_srs(instance_uuid))

        marc21 = marc_json_handler.records[0]

        return marc21

    def marc_json_from_srs(self, instance_uuid: str) -> str:
        srs_result = self.folio_client.folio_get(
            f"/source-storage/records/{instance_uuid}/formatted?idType=INSTANCE"
        )

        return srs_result["parsedRecord"]["content"]

    def write_marc(
        self,
        instance_file: pathlib.Path,
        marc_directory: Union[pathlib.Path, S3Path],
        marc: Union[list[marcRecord], marcRecord],
        kind: str,
        as_xml: bool = False,
    ) -> str:
        """
        Writes marc record to a file system (local or S3)
        as_xml writes MARC-XML, which has no record length limit
        """
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        marc_file_dir = params.get("marc_file_dir", "marc-files")
        marc_file_name = instance_file.stem
        directory = marc_directory / marc_file_dir
        mode = "wb"

        if type(marc_directory).__name__ == 'PosixPath':
            directory = directory / kind
            if not as_xml:
                mode = "ab"
                marc = [marc]  # type: ignore

        logger.info(f"Writing to directory: {directory}")
        directory.mkdir(parents=True, exist_ok=True)
        suffix = ".xml" if as_xml else ".mrc"
        marc_file = directory / f"{marc_file_name}{suffix}"

        with marc_file.open(mode) as fo:
            writer = marc_writer(fo, marc_file)
            for record in marc:
                writer.write(record)
            writer.close(close_fh=False)

        return str(marc_file.absolute())
