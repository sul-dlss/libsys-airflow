import json
import logging
import uuid

import httpx
import pymarc

from typing import List, Union

from libsys_airflow.plugins.folio_client import folio_client

logger = logging.getLogger(__name__)


class OCLCAPIWrapper(object):
    """
    Helper class for transmitting MARC records to OCLC Worldcat
    API
    """

    auth_url = "https://oauth.oclc.org/token?grant_type=client_credentials&scope=WorldCatMetadataAPI"
    worldcat_metadata_url = "https://metadata.api.oclc.org/worldcat"

    def __init__(self, **kwargs):
        self.oclc_headers = None
        user = kwargs["user"]
        password = kwargs["password"]
        self.snapshot = None
        self.__authenticate__(user, password)
        self.folio_client = folio_client()
        self.httpx_client = None
    

    def __authenticate__(self, username, passphrase) -> None:
        try:
            result = httpx.post(
                url=OCLCAPIWrapper.auth_url,
                auth=(username, passphrase),
            )
            logger.info("Retrieved API Access Token")
            token = result.json()["access_token"]
            self.oclc_headers = {
                "Authorization": token,
                "Content-type": "application/marc",
            }
        except Exception as e:
            logger.error("Retrieved API Access Token")
            raise Exception("Unable to Retrieve Access Token", e)

    def __generate_snapshot__(self) -> None:
        snapshot_uuid = str(uuid.uuid4())
        with httpx.Client() as client:
            post_result = client.post(
                f"{self.folio_client.okapi_url}/source-storage/snapshots",
                headers=self.folio_client.okapi_headers,
                json={"jobExecuteionId": snapshot_uuid, "status": "NEW"},
            )
            post_result.raise_for_status()
        self.snapshot = snapshot_uuid

    def __srs_uuid__(self, record) -> Union[str, None]:
        srs_uuid = None
        for field in record.get_fields("999"):
            if "s" in field.subfields and field.indicators == ["f", "f"]:
                srs_uuid = field["s"]
        if srs_uuid is None:
            logger.error("Record Missing SRS uuid")
        return srs_uuid

    def __update_035__(
        self, oclc_put_result: httpx.Response, record: pymarc.Record
    ) -> None:
        """
        Extracts 035 field with new OCLC number adds to existing MARC21
        record
        """
        oclc_record = pymarc.Record(oclc_put_result.content)
        fields_035 = oclc_record.get_fields('035')
        for field in fields_035:
            subfields_a = field.get_subfields("a")
            for subfield in subfields_a:
                if subfield.startswith("(OCoLC"):
                    record.add_ordered_field(field)
                    break

    def put_folio_record(self, srs_uuid: str, record: pymarc.Record) -> bool:
        """
        Updates FOLIO SRS with updated MARC record with new OCLC Number
        in the 035 field
        """
        marc_json = record.as_json()
        if self.snapshot is None:
            self.__generate_snapshot__()
    
        put_result = self.httpx_client.put(
            f"{self.folio_client.okapi_url}/source-storage/records/{srs_uuid}",
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

    def new(self, marc_records: List[pymarc.Record]) -> dict:
        output = {"success": [], "failures": []}
        if len(marc_records) < 1:
            logger.info("No new marc records")
            return output
        with httpx.Client() as client:
            self.httpx_client = client
            for record in marc_records:
                srs_uuid = self.__srs_uuid__(record)
                if srs_uuid is None:
                    continue
                new_record_result = self.httpx_client.post(
                    f"{OCLCAPIWrapper.worldcat_metadata_url}/manage/bibs",
                    headers=self.oclc_headers,
                    data=record,
                )
                if new_record_result.status_code != 200:
                    logger.error(
                        f"Failed to create record, error: {new_record_result.text}"
                    )
                    output['failures'].append(srs_uuid)
                    continue
                self.__update_035__(new_record_result.content, record)
                if not self.put_folio_record(srs_uuid, record):
                    output['failures'].append(srs_uuid)
                    continue
                output['success'].append(srs_uuid)
        return output

    def update(self, marc_records: List[pymarc.Record]):
        output = {"success": [], "failures": []}
        if len(marc_records) < 1:
            logger.info("No updated marc records")
            return output
        with httpx.Client() as client:
            self.httpx_client = client
            for record in marc_records:
                srs_uuid = self.__srs_uuid__(record)
                if srs_uuid is None:
                    continue
                put_result = self.httpx_client.put(
                    f"{OCLCAPIWrapper.worldcat_metadata_url}/manage/institution/holdings/set",
                    headers=self.oclc_headers,
                    data=record,
                )
                if put_result.status_code != 200:
                    logger.error(f"Failed to create record, error: {put_result.text}")
                    output['failures'].append(srs_uuid)
                    continue
                #! Need to update OCLC code in 035 field
                output['success'].append(srs_uuid)
        return output
