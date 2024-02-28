import csv
import pathlib

import pymarc

from airflow.models import Variable
from folioclient import FolioClient


def _check_001(field001s: list) -> bool:
    reject = False
    for field in field001s:
        if field.value() == "gls":
            reject = True
    return reject


def _check_008(fields008: list) -> bool:
    reject = False
    for field in fields008:
        lang_code = field.value()[35:37]
        if lang_code not in ["eng", "fre"]:
            reject = True
    return reject


def _check_590(field590s: list) -> bool:
    reject = False
    for field in field590s:
        if "MARCit brief record" in field.get_subfields("a"):
            reject = True
    return reject


def _check_915(fields915: list) -> bool:
    reject = False
    for field in fields915:
        if "NO EXPORT" in field.get_subfields(
            "a"
        ) and "FOR SU ONLY" in field.get_subfields("b"):
            reject = True
    return reject


def _exclude_marc_by_vendor(marc_record: pymarc.Record, vendor: str):
    """
    Filters MARC record by Vendor
    """
    exclude = False
    match vendor:
        case "gobi":
            exclude = any(
                [
                    _check_001(marc_record.get_fields("001")),
                    _check_008(marc_record.get_fields("008")),
                ]
            )

        case "oclc":
            exclude = any(
                [
                    _check_590(marc_record.get_fields("590")),
                    _check_915(marc_record.get_fields("915")),
                ]
            )

        case "pod":
            exclude = any(
                [
                    _check_590(marc_record.get_fields("590")),
                    _check_915(marc_record.get_fields("915")),
                ]
            )

    return exclude


def _marc_from_srs(instance_uuid: str, folio_client: FolioClient) -> pymarc.Record:
    """
    Retrieve MARC JSON from SRS based instance UUID, converts and returns
    MARC21 record
    """
    srs_result = folio_client.folio_get(
        f"/change-manager/parsedRecords?externalId={instance_uuid}"
    )

    marc_json_record = srs_result["parsedRecord"]["content"]

    json_handler = pymarc.JSONHandler()

    json_handler.elements(marc_json_record)

    marc21 = json_handler.records[0]

    return marc21


def fetch_marc(**kwargs) -> str:
    """
    Opens list of instances UUID, retrieves MARC JSON from SRS,
    and writes records as MARC21
    """
    instance_filepath = kwargs["instance_file"]

    instance_file = pathlib.Path(instance_filepath)

    if not instance_file.exists():
        raise ValueError(f"{instance_filepath} does not exist")

    stem, vendor_name = instance_file.stem, instance_file.parent.parent.name

    folio_client = kwargs.get("folio_client")

    if folio_client is None:
        folio_client = FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )

    marc_directory = instance_file.parent.parent / "Marc files"
    marc_directory.mkdir(parents=True, exist_ok=True)

    marc_file = marc_directory / f"{stem}.mrc"
    marc_writer = pymarc.MARCWriter(marc_file.open("wb+"))

    with instance_file.open() as fo:
        instance_reader = csv.reader(fo)
        for row in instance_reader:
            uuid = row[1]
            marc_record = _marc_from_srs(uuid, folio_client)
            if _exclude_marc_by_vendor(marc_record, vendor_name):
                continue
            marc_writer.write(marc_record)

    marc_writer.close()
    return str(marc_file.absolute())
