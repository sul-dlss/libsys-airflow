import csv
import logging
import pathlib
from pymarc import (
    JSONHandler as marcJson,
    MARCWriter as marcWriter,
    Record as marcRecord,
)

from libsys_airflow.plugins.shared.folio_client import folio_client
from airflow.models import Variable
from s3path import S3Path
from typing import Union

logger = logging.getLogger(__name__)


class Exporter(object):
    def __init__(self):
        self.folio_client = folio_client()

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
            if any("MARCit brief record" in sf for sf in field.get_subfields("a")):
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

            case "oclc" | "pod" | "sharevde" | "full-dump":
                exclude = any(
                    [
                        self.check_590(marc_record.get_fields("590")),
                        self.check_915(marc_record.get_fields("915")),
                    ]
                )

        return exclude

    def retrieve_marc_for_instances(
        self, instance_file: pathlib.Path, kind: str
    ) -> str:
        """
        Called for each instanceid file in vendor or full-dump directory
        For each ID row, writes and returns converted MARC from SRS
        Writes to file system, or in case of full-dump to AWS bucket
        """
        if not instance_file.exists():
            raise ValueError(
                "Instance file does not exist for retrieve_marc_for_instances"
            )

        vendor_name = instance_file.parent.parent.parent.name

        marc_file = ""
        with instance_file.open() as fo:
            instance_reader = csv.reader(fo)
            for row in instance_reader:
                uuid = row[0]
                try:
                    marc_record = self.marc21(uuid)
                except Exception as e:
                    logger.warning(e)
                    continue

                if self.exclude_marc_by_vendor(marc_record, vendor_name):
                    logger.info(f"Excluding {vendor_name}")
                    continue

                marc_directory = instance_file.parent.parent.parent
                marc_file = self.write_marc(
                    instance_file, marc_directory, marc_record, kind
                )

        return marc_file

    def retrieve_marc_for_full_dump(self, marc_filename: str, instance_ids: str) -> str:
        marc_file = ""
        bucket = Variable.get("FOLIO_AWS_BUCKET", "folio-data-export-prod")
        full_dump_files = f"/{bucket}/data-export-files/full-dump"

        marc = []
        for row in instance_ids:
            marc_json_handler = marcJson()
            try:
                marc_json_handler.elements(row[1])
                marc21 = marc_json_handler.records[0]
            except Exception as e:
                logger.warning(e)
                continue

            if self.exclude_marc_by_vendor(marc21, "full-dump"):
                continue

            marc.append(marc21)

        logger.info(
            f"Saving {len(instance_ids)} marc records to {marc_filename} in bucket."
        )
        marc_file = self.write_marc(
            pathlib.Path(marc_filename), S3Path(full_dump_files), marc, "."
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
    ) -> str:
        """
        Writes marc record to a file system (local or S3)
        """
        marc_file_name = instance_file.stem
        directory = marc_directory / "marc-files"
        mode = "wb"

        if type(marc_directory).__name__ == 'PosixPath':
            mode = "ab"
            marc = [marc]  # type: ignore
            directory = directory / kind

        logger.info(f"Writing to directory: {directory}")
        directory.mkdir(parents=True, exist_ok=True)
        marc_file = directory / f"{marc_file_name}.mrc"
        marc_file.touch()

        with marc_file.open(mode) as fo:
            marc_writer = marcWriter(fo)
            for record in marc:
                marc_writer.write(record)

        marc_writer.close()

        return str(marc_file.absolute())
