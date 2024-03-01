import logging
import pathlib

import pymarc

from libsys_airflow.plugins.data_exports.marc.transformer import Transformer

logger = logging.getLogger(__name__)


class OCLCTransformer(Transformer):
    def __init__(self):
        super().__init__()
        self.library_lookup = self.locations_by_library_lookup()
        self.libraries = {}
        for code in ["CASUM", "HIN", "RCJ", "S7Z", "STF"]:
            self.libraries[code] = {"holdings": [], "marc": []}
        self.marc_path = None
        self.staff_notices = []

    def determine_campus_code(self, record: pymarc.Record):
        instance_uuid = record["999"].get_subfields("i")[0]
        holdings_result = self.folio_client.folio_get(
            f"/holdings-storage/holdings?query=(instanceId=={instance_uuid})"
        )
        codes = []
        for holding in holdings_result['holdingsRecords']:
            library = self.library_lookup.get(holding.get('permanentLocationId'))
            if library is None:
                continue
            match library:
                case "BUSINESS":
                    oclc_code = "S7Z"

                case "HILA":
                    oclc_code = "HIN"

                case "LANE":
                    oclc_code = "CASUM"

                case "LAW":
                    oclc_code = "RCJ"

                case _:
                    oclc_code = "STF"

            codes.append(oclc_code)
        return codes

    def divide(self, marc_file):
        """
        Divides up MARC Export by Campus and presence of OCLC record id
        """
        self.marc_path = pathlib.Path(marc_file)

        with self.marc_path.open('rb') as fo:
            marc_records = [record for record in pymarc.MARCReader(fo)]

        logging.info(f"Process {len(marc_records):,} record for OCLC data export")

        for i, record in enumerate(marc_records):
            if not i % 100:
                logging.info(f"{i:,} records processed")

            record_ids = self.get_record_id(record)
            campus_codes = self.determine_campus_code(record)
            for code in campus_codes:
                match len(record_ids):
                    case 0:
                        self.libraries[code]["marc"].append(record)

                    case 1:
                        self.libraries[code]["holdings"].append(record)

                    case _:
                        self.multiple_codes(record, code, record_ids)

    def get_record_id(self, record: pymarc.Record) -> list:
        """
        Extracts OCLC id from 035 field
        """
        oclc_ids = []
        for field in record.get_fields("035"):
            subfields_a = field.get_subfields("a")
            for subfield in subfields_a:
                if subfield.startswith("(OCoLC"):
                    oclc_ids.append(subfield)
        return oclc_ids

    def multiple_codes(self, record: pymarc.Record, code: str, record_ids: list):
        instance_id = record['999']['i']
        self.staff_notices.append((instance_id, code, record_ids))

    def locations_by_library_lookup(self) -> dict:
        lookup = {}
        libraries_result = self.folio_client.folio_get(
            "/location-units/libraries?limit=1000"
        )
        libraries_lookup = {}
        for library in libraries_result.get("loclibs"):
            libraries_lookup[library['id']] = library["code"]
        for location in self.folio_client.locations:
            lookup[location['id']] = libraries_lookup.get(location['libraryId'])
        return lookup

    def save(self):
        """
        Saves existing holdings and marc records to file system
        """

        def _save_file(records: list, file_name: str):
            marc_file_path = oclc_parent / file_name
            with marc_file_path.open("wb+") as fo:
                marc_writer = pymarc.MARCWriter(fo)
                for record in records:
                    marc_writer.write(record)

        oclc_parent = self.marc_path.parent
        for library_code, values in self.libraries.items():
            files_stem = f"{self.marc_path.stem}-{library_code}"
            if len(values["marc"]) > 0:
                _save_file(values["marc"], f"{files_stem}-new.mrc")
                logger.info(f"Export new records for {library_code}")
            if len(values["holdings"]) > 0:
                _save_file(values["holdings"], f"{files_stem}-update.mrc")
                logger.info(f"Updating records for {library_code}")
