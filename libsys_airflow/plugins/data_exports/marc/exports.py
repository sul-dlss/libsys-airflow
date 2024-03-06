import csv
import logging
import pathlib

import pymarc

from airflow.models import Variable
from folioclient import FolioClient

logger = logging.getLogger(__name__)


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

        case "oclc" | "pod" | "sharevde":
            exclude = any(
                [
                    _check_590(marc_record.get_fields("590")),
                    _check_915(marc_record.get_fields("915")),
                ]
            )

    return exclude


def _folio_client(**kwargs):
    _client = kwargs.get("client")

    if _client is None:
        _client = FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )

    return _client


def _marc21_from_srs(instance_uuid: str, folio_client: FolioClient) -> pymarc.Record:
    """
    Retrieve MARC JSON from SRS based instance UUID, converts and returns
    MARC21 record
    """
    srs_result = folio_client.folio_get(
        #  f"/change-manager/parsedRecords?externalId={instance_uuid}"
        f"/source-storage/records/{instance_uuid}/formatted?idType=INSTANCE"
    )

    marc_json_record = srs_result["parsedRecord"]["content"]

    json_handler = pymarc.JSONHandler()

    json_handler.elements(marc_json_record)

    marc21 = json_handler.records[0]

    return marc21


def instance_files_dir(**kwargs) -> list[pathlib.Path]:
    """
    Finds the instance id directory for a vendor
    """
    airflow = kwargs.get("airflow", "/opt/airflow")
    vendor = kwargs.get("vendor")
    instance_dir = pathlib.Path(airflow) / f"data-export-files/{vendor}/instanceids"
    instance_files = list(instance_dir.glob("*.csv"))

    if not instance_files:
        raise ValueError("Vendor instance files do not exist")

    return instance_files


def marc_for_instances(**kwargs) -> list[str]:
    """
    Retrieves the converted marc for each instance id file in vendor directory
    """
    instance_files = instance_files_dir(
        airflow=kwargs.get("airflow", "/opt/airflow"), vendor=kwargs.get("vendor")
    )

    for file_datename in instance_files:
        retrieve_marc_for_instances(
            instance_file=file_datename,
            folio_client=_folio_client(client=kwargs.get("folio_client")),
        )

    return [str(f) for f in instance_files]


def retrieve_marc_for_instances(**kwargs) -> None:
    """
    Writes and returns converted MARC from SRS
    """

    instance_file = pathlib.Path(kwargs.get("instance_file", ""))
    if not instance_file.exists():
        raise ValueError("Instance file does not exist for retrieve_marc_for_instances")

    vendor_name = instance_file.parent.parent.name

    with instance_file.open() as fo:
        instance_reader = csv.reader(fo)
        for row in instance_reader:
            uuid = row[0]
            try:
                marc_record = _marc21_from_srs(
                    uuid, _folio_client(client=kwargs.get("folio_client"))
                )
            except Exception as e:
                logger.warning(e)
                continue

            if _exclude_marc_by_vendor(marc_record, vendor_name):
                logger.info(f"Excluding {vendor_name}")
                continue

            write_marc(
                instance_file=instance_file,
                marc_directory=instance_file.parent.parent,
                marc_record=marc_record,
            )


def write_marc(**kwargs):
    """
    Writes marc record to file system
    """
    instance_file = kwargs.get("instance_file")
    marc_file_name = instance_file.stem
    marc_directory = kwargs.get("marc_directory") / "marc-files"
    marc_directory.mkdir(parents=True, exist_ok=True)
    marc_file = marc_directory / f"{marc_file_name}.mrc"
    marc_writer = pymarc.MARCWriter(marc_file.open("ab"))
    marc_writer.write(kwargs.get("marc_record"))
    marc_writer.close()
    return str(marc_file.absolute())
