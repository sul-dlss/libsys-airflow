import logging
import pathlib

import pymarc

from libsys_airflow.plugins.data_exports.marc.transformer import Transformer

logger = logging.getLogger(__name__)


def archive_instanceid_csv(instance_id_csvs: list):
    for instance_id_csv in instance_id_csvs:
        csv_file_path = pathlib.Path(instance_id_csv)
        if csv_file_path.exists():
            kind = csv_file_path.parent.name
            archive_dir = csv_file_path.parent.parent.parent / "transmitted" / kind
            archive_dir.mkdir(parents=True, exist_ok=True)
            archive_instance_ids_path = archive_dir / csv_file_path.name
            csv_file_path.replace(archive_instance_ids_path)
            logger.info(f"Archived {csv_file_path} to {archive_instance_ids_path}")


def get_record_id(record: pymarc.Record) -> list:
    """
    Extracts OCLC control number from 035 field
    """
    oclc_ids = set()
    for field in record.get_fields("035"):
        subfields_a = field.get_subfields("a")
        for subfield in subfields_a:
            # Skip Legacy OCLC Number
            if subfield.startswith("(OCoLC-I)"):
                continue
            # Matches (OCoLC) and (OCoLC-M)
            if subfield.startswith("(OCoLC"):
                raw_oclc_number = subfield.split(")")[-1].strip()
                if raw_oclc_number.startswith("ocm") or raw_oclc_number.startswith(
                    "ocn"
                ):
                    oclc_number = raw_oclc_number[3:]
                elif raw_oclc_number.startswith("on"):
                    oclc_number = raw_oclc_number[2:]
                else:
                    oclc_number = raw_oclc_number
                oclc_ids.add(oclc_number)
    return list(oclc_ids)


class OCLCTransformer(Transformer):
    def __init__(self):
        super().__init__()
        self.libraries = {}
        for code in ["CASUM", "HIN", "RCJ", "S7Z", "STF"]:
            self.libraries[code] = {"holdings": {}, "marc": {}}
        self.staff_notices = []

    def __filter_999__(self, record: pymarc.Record) -> str:
        """
        Filters 999 fields to extract FOLIO Instance UUID
        """
        fields999 = record.get_fields("999")
        instance_uuid = ""
        for field in fields999:
            if field.indicators == ["f", "f"]:
                instance_uuid = field.get_subfields("i")[0]
                break
        return instance_uuid

    def determine_campus_code(self, record: pymarc.Record):
        instance_uuid = self.__filter_999__(record)

        holdings_result = self.folio_client.folio_get(
            f"/holdings-storage/holdings?query=(instanceId=={instance_uuid})"
        )
        codes = []
        for holding in holdings_result['holdingsRecords']:
            campus = self.campus_lookup.get(holding.get('permanentLocationId'))
            if campus is None:
                continue
            match campus:
                case "GSB":
                    oclc_code = "S7Z"

                case "HOOVER":
                    oclc_code = "HIN"

                case "MED":
                    oclc_code = "CASUM"

                case "LAW":
                    oclc_code = "RCJ"

                case _:
                    oclc_code = "STF"

            codes.append(oclc_code)
        return codes

    def divide(self, marc_file) -> None:
        """
        Divides up MARC Export by Campus and presence of OCLC record id
        """
        marc_path = pathlib.Path(marc_file)

        with marc_path.open('rb') as fo:
            marc_records = [record for record in pymarc.MARCReader(fo)]

        logger.info(f"Process {len(marc_records):,} record for OCLC data export")

        for i, record in enumerate(marc_records):
            if not i % 100:
                logger.info(f"{i:,} records processed")

            record_ids = get_record_id(record)
            campus_codes = self.determine_campus_code(record)

            file_path = str(marc_path)
            for code in campus_codes:
                match len(record_ids):
                    case 0:
                        if file_path not in self.libraries[code]["marc"]:
                            self.libraries[code]["marc"][file_path] = []
                        self.libraries[code]["marc"][file_path].append(record)

                    case 1:
                        if file_path not in self.libraries[code]["holdings"]:
                            self.libraries[code]["holdings"][file_path] = []
                        self.libraries[code]["holdings"][file_path].append(record)

                    case _:
                        self.multiple_codes(record, code, record_ids)

    def multiple_codes(self, record: pymarc.Record, code: str, record_ids: list):
        instance_id = record['999']['i']
        self.staff_notices.append((instance_id, code, record_ids))

    def save(self):
        """
        Saves existing holdings and marc records to file system
        """

        def _save_file(records_by_file: dict, library_code: str, type_of: str):
            for file_path_key, records in records_by_file.items():
                file_path = pathlib.Path(file_path_key)
                # If "new" records actually are updates due to presence of an OCLC
                # number in the 035, saves the records in the "updates" directory
                if type_of.startswith("updates") and file_path.parent.name == "new":
                    parent = file_path.parents[1] / "updates"
                    parent.mkdir(parents=True, exist_ok=True)
                    # Adds trailing 'mv' to file path stem to avoid overwriting an existing
                    # updates file or being replaced by an incoming updates file
                    file_name = f"{file_path.stem}mv-{library_code}.mrc"
                else:
                    parent = file_path.parent
                    file_name = f"{file_path.stem}-{library_code}.mrc"
                marc_file_path = parent / file_name
                with marc_file_path.open("wb+") as fo:
                    marc_writer = pymarc.MARCWriter(fo)
                    for record in records:
                        marc_writer.write(record)
                original_marc_files.add(str(file_path))

        original_marc_files = set()
        for library_code, values in self.libraries.items():
            logger.info(f"Library code {library_code}")
            if len(values["marc"]) > 0:
                _save_file(values["marc"], library_code, "new")
                logger.info(f"Export new records for {library_code}")
            if len(values["holdings"]) > 0:
                _save_file(values["holdings"], library_code, "updates")
                logger.info(f"Updating records for {library_code}")
        return list(original_marc_files)
