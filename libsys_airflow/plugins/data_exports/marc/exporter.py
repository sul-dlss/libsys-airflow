import csv
import logging
import pathlib
import pymarc

from libsys_airflow.plugins.folio_client import folio_client


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
            if any("NO EXPORT" in sf for sf in field.get_subfields(
                "a"
            )) and any("FOR SU ONLY" in sf for sf in field.get_subfields("b")):
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

            case "oclc" | "pod" | "sharevde":
                exclude = any(
                    [
                        self.check_590(marc_record.get_fields("590")),
                        self.check_915(marc_record.get_fields("915")),
                    ]
                )

        return exclude

    def retrieve_marc_for_instances(self, instance_file: pathlib.Path) -> None:
        """
        Writes and returns converted MARC from SRS
        """
        if not instance_file.exists():
            raise ValueError(
                "Instance file does not exist for retrieve_marc_for_instances"
            )

        vendor_name = instance_file.parent.parent.name

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

                self.write_marc(instance_file, instance_file.parent.parent, marc_record)

    def marc21(self, instance_uuid: str) -> pymarc.Record:
        json_handler = pymarc.JSONHandler()

        json_handler.elements(self.marc_json_from_srs(instance_uuid))

        marc21 = json_handler.records[0]

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
