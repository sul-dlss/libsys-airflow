import json
import logging
import pathlib
import uuid

import httpx
import pymarc

from typing import List, Union

from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import WorldcatRequestError

from libsys_airflow.plugins.data_exports.marc.oclc import get_record_id
from libsys_airflow.plugins.data_exports.marc.transforms import oclc_excluded

from libsys_airflow.plugins.folio_client import folio_client

logger = logging.getLogger(__name__)


class OCLCAPIWrapper(object):
    # Helper class for transmitting MARC records to OCLC Worldcat API

    auth_url = "https://oauth.oclc.org/token?grant_type=client_credentials&scope=WorldCatMetadataAPI"
    worldcat_metadata_url = "https://metadata.api.oclc.org/worldcat"

    def __init__(self, **kwargs):
        self.oclc_token = None
        client_id = kwargs["client_id"]
        secret = kwargs["secret"]
        self.httpx_client = httpx.Client()
        self.snapshot = None
        self.__authenticate__(client_id, secret)
        self.folio_client = folio_client()

    def __del__(self):
        if self.snapshot:
            self.__close_snapshot__()
        self.httpx_client.close()

    def __authenticate__(self, client_key, secret) -> None:
        try:
            self.oclc_token = WorldcatAccessToken(
                key=client_key, secret=secret, scopes="WorldCatMetadataAPI"
            )
        except Exception as e:
            msg = "Unable to Retrieve Worldcat Access Token"
            logger.error(msg)
            raise Exception(msg, e)

    def __close_snapshot__(self) -> None:
        post_result = self.httpx_client.post(
            f"{self.folio_client.okapi_url}source-storage/snapshots",
            headers=self.folio_client.okapi_headers,
            json={"jobExecutionId": self.snapshot, "status": "PROCESSING_FINISHED"},
        )

        post_result.raise_for_status()

    def __generate_snapshot__(self) -> None:
        snapshot_uuid = str(uuid.uuid4())
        post_result = self.httpx_client.post(
            f"{self.folio_client.okapi_url}source-storage/snapshots",
            headers=self.folio_client.okapi_headers,
            json={"jobExecutionId": snapshot_uuid, "status": "NEW"},
        )
        post_result.raise_for_status()
        self.snapshot = snapshot_uuid

    def __get_srs_record__(self, srs_uuid: str) -> Union[pymarc.Record, None]:
        marc_json = self.folio_client.folio_get(f"/source-storage/records/{srs_uuid}")
        marc_json_handler = pymarc.JSONHandler()
        try:
            marc_json_handler.elements(marc_json)
            return marc_json_handler.records[0]
        except KeyError as e:
            logger.error(f"Failed converting {srs_uuid} to MARC JSON {e}")
            return None

    def __put_folio_record__(self, srs_uuid: str, record: pymarc.Record) -> bool:
        """
        Updates FOLIO SRS with updated MARC record with new OCLC Number
        in the 035 field
        """
        marc_json = record.as_json()
        if self.snapshot is None:
            self.__generate_snapshot__()

        put_result = self.httpx_client.put(
            f"{self.folio_client.okapi_url}source-storage/records/{srs_uuid}",
            headers=self.folio_client.okapi_headers,
            json={
                "snapshotId": self.snapshot,
                "matchedId": srs_uuid,
                "recordType": "MARC_BIB",
                "rawRecord": {"content": json.dumps(marc_json)},
                "parsedRecord": {"content": marc_json},
            },
        )
        if put_result.status_code != 200:
            logger.error(f"Failed to update FOLIO for SRS {srs_uuid}")
            return False
        return True

    def __read_marc_files__(self, marc_files: list) -> list:
        records = []
        for marc_file in marc_files:
            marc_file_path = pathlib.Path(marc_file)
            if marc_file_path.exists():
                with marc_file_path.open('rb') as fo:
                    marc_reader = pymarc.MARCReader(fo)
                    records.extend([r for r in marc_reader])
        return records

    def __record_uuids__(self, record) -> tuple:
        instance_uuid, srs_uuid = None, None
        for field in record.get_fields("999"):
            if field.indicators == ["f", "f"]:
                srs_uuid = field["s"]
                instance_uuid = field["i"]
        if srs_uuid is None:
            logger.error("Record Missing SRS uuid")
        return instance_uuid, srs_uuid

    def __update_035__(self, oclc_put_result: bytes, srs_uuid: str) -> bool:
        """
        Extracts 035 field with new OCLC number and adds to existing MARC21
        record
        """
        record = self.__get_srs_record__(srs_uuid)
        if record is None:
            return False
        oclc_record = pymarc.Record(data=oclc_put_result)  # type: ignore
        fields_035 = oclc_record.get_fields('035')
        for field in fields_035:
            for subfield in field.get_subfields("a"):
                if subfield.startswith("(OCoLC"):
                    record.add_ordered_field(field)
                    break
        return self.__put_folio_record__(srs_uuid, record)

    def __update_oclc_number__(self, control_number: str, srs_uuid: str) -> bool:
        """
        Updates 035 field if control_number has changed
        """
        record = self.__get_srs_record__(srs_uuid)
        if record is None:
            return False
        for field in record.get_fields('035'):
            for subfield in field.get_subfields("a"):
                if control_number in subfield:
                    return True  # Control number already exists
        new_035 = pymarc.Field(
            tag='035',
            indicators=[' ', ' '],
            subfields=[pymarc.Subfield(code='a', value=f"(OCoLC){control_number}")],
        )
        record.add_ordered_field(new_035)
        return self.__put_folio_record__(srs_uuid, record)

    def new(self, marc_files: List[str]) -> dict:
        output: dict = {"success": [], "failures": []}
        if len(marc_files) < 1:
            logger.info("No new marc records")
            return output
        marc_records = self.__read_marc_files__(marc_files)

        with MetadataSession(authorization=self.oclc_token) as session:
            for record in marc_records:
                instance_uuid, srs_uuid = self.__record_uuids__(record)
                if srs_uuid is None:
                    continue
                try:
                    record.remove_fields(*oclc_excluded)
                    marc21 = record.as_marc21()
                    session.bib_validate(
                        record=marc21,
                        recordFormat="application/marc",
                        validationLevel="validateFull",
                    )
                    new_record = session.bib_create(
                        record=marc21,
                        recordFormat="application/marc",
                    )
                    if self.__update_035__(new_record.text, srs_uuid):  # type: ignore
                        output['success'].append(instance_uuid)
                    else:
                        output['failures'].append(instance_uuid)
                except WorldcatRequestError as e:
                    logger.error(e)
                    output['failures'].append(instance_uuid)
                    continue
        return output

    def update(self, marc_files: List[str]):
        output: dict = {"success": [], "failures": []}
        if len(marc_files) < 1:
            logger.info("No updated marc records")
            return output
        marc_records = self.__read_marc_files__(marc_files)

        with MetadataSession(authorization=self.oclc_token) as session:
            for record in marc_records:
                instance_uuid, srs_uuid = self.__record_uuids__(record)
                if srs_uuid is None:
                    continue
                oclc_id = get_record_id(record)
                match len(oclc_id):

                    case 0:
                        logger.error(f"{srs_uuid} missing OCLC number")
                        output['failures'].append(instance_uuid)
                        continue

                    case 1:
                        pass

                    case _:
                        logger.error(f"Multiple OCLC ids for {srs_uuid}")
                        output['failures'].append(instance_uuid)
                        continue

                try:
                    response = session.holdings_set(oclcNumber=oclc_id[0])
                    if response is None:
                        output['failures'].append(instance_uuid)
                        continue
                    if self.__update_oclc_number__(
                        response.json()['controlNumber'], srs_uuid
                    ):
                        output['success'].append(instance_uuid)
                    else:
                        output['failures'].append(instance_uuid)
                except WorldcatRequestError as e:
                    logger.error(f"Failed to update record, error: {e}")
                    output['failures'].append(instance_uuid)
                    continue
        return output
