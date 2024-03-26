import csv
import io
import logging
import pathlib
import pymarc

from libsys_airflow.plugins.folio_client import folio_client
from airflow.operators.python import get_current_context
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator


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
            lang_code = field.value()[35:37]
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

    def exclude_marc_by_vendor(self, marc_record: pymarc.Record, vendor: str):
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

    def retrieve_marc_for_instances(self, instance_file: pathlib.Path) -> str:
        """
        Called for each instanceid file in vendor or full-dump directory
        For each ID row, writes and returns converted MARC from SRS
        Writes to file system, or in case of full-dump to AWS bucket
        """
        if not instance_file.exists():
            raise ValueError(
                "Instance file does not exist for retrieve_marc_for_instances"
            )

        vendor_name = instance_file.parent.parent.name

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

                marc_file = self.write_marc(
                    instance_file, instance_file.parent.parent, marc_record
                )

        return marc_file

    def retrieve_marc_for_full_dump(self, marc_file: str, instance_ids: str) -> None:
        marc_records = []
        for row in instance_ids:
            uuid = row[0]
            try:
                marc = self.marc21(uuid)
            except Exception as e:
                logger.warning(e)
                continue

            if self.exclude_marc_by_vendor(marc, "full-dump"):
                logger.info("Excluding full-dump")
                continue

            marc_records.append(marc)

        self.write_s3_marc(marc_file, marc_records)

    def marc21(self, instance_uuid: str) -> pymarc.Record:
        marc_json_handler = pymarc.JSONHandler()

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
        marc_directory: pathlib.Path,
        marc_record: pymarc.Record,
    ):
        """
        Writes marc record to file system
        """
        marc_file_name = instance_file.stem
        directory = marc_directory / "marc-files"
        directory.mkdir(parents=True, exist_ok=True)
        marc_file = directory / f"{marc_file_name}.mrc"
        marc_writer = pymarc.MARCWriter(marc_file.open("ab"))
        marc_writer.write(marc_record)
        marc_writer.close()
        return str(marc_file.absolute())

    def write_s3_marc(self, marc_file, marc_records):
        context = get_current_context()
        marc_data = io.BytesIO()
        writer = pymarc.MARCWriter(marc_data)
        for mrec in marc_records:
            writer.write(mrec)

        writer.close(close_fh=False)

        logger.info(
            f"Saving {len(marc_records)} marc records to {marc_file} in bucket."
        )
        S3CreateObjectOperator(
            task_id="create_object",
            s3_bucket=Variable.get("FOLIO_AWS_BUCKET", "folio-data-export-prod"),
            s3_key=f"full-dump-marc-files/{marc_file}",
            data=marc_data.getvalue(),
            replace=True,
        ).execute(context)
