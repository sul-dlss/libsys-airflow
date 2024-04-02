import json
import logging
import pathlib
import uuid

import httpx
import pymarc

from typing import List, Union

from libsys_airflow.plugins.folio_client import folio_client

logger = logging.getLogger(__name__)


class OCLCAPIWrapper(object):
    # Helper class for transmitting MARC records to OCLC Worldcat API

    auth_url = "https://oauth.oclc.org/token?grant_type=client_credentials&scope=WorldCatMetadataAPI"
    worldcat_metadata_url = "https://metadata.api.oclc.org/worldcat"

    def __init__(self, **kwargs):
        self.oclc_headers = None
        user = kwargs["user"]
        password = kwargs["password"]
        self.snapshot = None
        self.httpx_client = None
        self.__authenticate__(user, password)
        self.folio_client = folio_client()

    def __del__(self):
        if self.httpx_client:
            self.httpx_client.close()
        if self.snapshot:
            self.__close_snapshot__()

    def __authenticate__(self, username, passphrase) -> None:
        try:
            self.httpx_client = httpx.Client()

            result = self.httpx_client.post(
                url=OCLCAPIWrapper.auth_url, auth=(username, passphrase)
            )
            logger.info("Retrieved API Access Token")
            token = result.json()["access_token"]
            self.oclc_headers = {
                "Authorization": f"Bearer: {token}",
                "Content-type": "application/marc",
            }
        except Exception as e:
            msg = "Unable to Retrieve Access Token"
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

    def __read_marc_files__(self, marc_files: list) -> list:
        records = []
        for marc_file in marc_files:
            marc_file_path = pathlib.Path(marc_file)
            if marc_file_path.exists():
                with marc_file_path.open('rb') as fo:
                    marc_reader = pymarc.MARCReader(fo)
                    records.extend([r for r in marc_reader])
        return records

    def __srs_uuid__(self, record) -> Union[str, None]:
        srs_uuid = None
        for field in record.get_fields("999"):
            if field.indicators == ["f", "f"]:
                srs_uuid = field["s"]
        if srs_uuid is None:
            logger.error("Record Missing SRS uuid")
        return srs_uuid

    def __update_035__(self, oclc_put_result: bytes, record: pymarc.Record) -> None:
        """
        Extracts 035 field with new OCLC number adds to existing MARC21
        record
        """
        oclc_record = pymarc.Record(data=oclc_put_result)  # type: ignore
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

    def new(self, marc_files: List[str]) -> dict:
        output: dict = {"success": [], "failures": []}
        if len(marc_files) < 1:
            logger.info("No new marc records")
            return output
        marc_records = self.__read_marc_files__(marc_files)

        for record in marc_records:
            srs_uuid = self.__srs_uuid__(record)
            if srs_uuid is None:
                continue
            new_record_result = self.httpx_client.post(
                f"{OCLCAPIWrapper.worldcat_metadata_url}/manage/bibs",
                headers=self.oclc_headers,
                data=record.as_marc21(),
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

    def update(self, marc_files: List[str]):
        output: dict = {"success": [], "failures": []}
        if len(marc_files) < 1:
            logger.info("No updated marc records")
            return output
        marc_records = self.__read_marc_files__(marc_files)

        for record in marc_records:
            srs_uuid = self.__srs_uuid__(record)
            if srs_uuid is None:
                continue
            post_result = self.httpx_client.post(
                f"{OCLCAPIWrapper.worldcat_metadata_url}/manage/institution/holdings/set",
                headers=self.oclc_headers,
                data=record.as_marc21(),
            )
            if post_result.status_code != 200:
                logger.error(f"Failed to update record, error: {post_result.text}")
                output['failures'].append(srs_uuid)
                continue
            # !Need to update OCLC code in 035 field
            output['success'].append(srs_uuid)
        return output
