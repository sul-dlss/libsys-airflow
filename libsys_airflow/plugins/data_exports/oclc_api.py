import copy
import json
import logging
import pathlib
import re

import httpx
import pymarc

from typing import List, Union, Callable

from bookops_worldcat import WorldcatAccessToken, MetadataSession
from bookops_worldcat.errors import WorldcatRequestError

from libsys_airflow.plugins.data_exports.marc.oclc import get_record_id
from libsys_airflow.plugins.data_exports.marc.excluded_tags import oclc_excluded

from libsys_airflow.plugins.shared.folio_client import folio_client

from pymarc import Record

logger = logging.getLogger(__name__)

# If we want to also match on OCoLC-I, just replace -[M] with -[MI]
OCLC_REGEX = re.compile(r"\(OCoLC(-[M])?\)(\w+)")


def get_instance_uuid(record) -> Union[str, None]:
    instance_uuid = None
    for field in record.get_fields("999"):
        if field.indicators == pymarc.Indicators(first="f", second="f"):
            instance_uuid = field["i"]
    if instance_uuid is None:
        logger.error("No instance UUID found in MARC record")
    return instance_uuid


def oclc_records_operation(**kwargs) -> dict:

    function_name: str = kwargs["oclc_function"]
    connection_lookup: dict = kwargs["connections"]

    type_of_records: dict = kwargs["records"]
    success: dict = kwargs.get("success", {})
    failures: dict = kwargs.get("failures", {})

    for library, records in type_of_records.items():
        success[library] = []
        failures[library] = []

        oclc_api = OCLCAPIWrapper(
            client_id=connection_lookup[library]["username"],
            secret=connection_lookup[library]["password"],
        )

        oclc_api_function = getattr(oclc_api, function_name)

        success[library] = []
        failures[library] = []
        archive_files = []
        if len(records) > 0:
            oclc_result = oclc_api_function(records)
            success[library].extend(oclc_result['success'])
            failures[library].extend(oclc_result['failures'])
            archive_files.extend(oclc_result['archive'])
            logger.info(
                f"Processed {function_name} for {library} successful {len(success[library])} failures {len(failures[library])}"
            )
        else:
            logger.info(f"No {function_name} records for {library}")

    return {"success": success, "failures": failures, "archive": archive_files}


class OCLCAPIWrapper(object):
    # Helper class for transmitting MARC records to OCLC Worldcat API

    def __init__(self, **kwargs):
        self.oclc_token = None
        client_id = kwargs["client_id"]
        secret = kwargs["secret"]
        self.httpx_client = httpx.Client()
        self.__authenticate__(client_id, secret)
        self.folio_client = folio_client()

    def __authenticate__(self, client_key, secret) -> None:
        try:
            self.oclc_token = WorldcatAccessToken(
                key=client_key, secret=secret, scopes="WorldCatMetadataAPI"
            )
        except Exception as e:
            msg = "Unable to Retrieve Worldcat Access Token"
            logger.error(msg)
            raise Exception(msg, e)

    def __get_srs_record_id__(self, instance_uuid: str) -> Union[str, None]:
        source_storage_result = self.folio_client.folio_get(
            f"/source-storage/source-records?instanceId={instance_uuid}"
        )

        try:
            source_records = source_storage_result['sourceRecords']
            if len(source_records) < 1:
                logger.error(f"No Active SRS record found for {instance_uuid}")
                return None
            return source_records[0]['recordId']

        except Exception as e:
            logger.error(
                f"Failed to retrieve Active SRS record id for Instance {instance_uuid} error: {e}"
            )
            return None

    def __instance_info__(self, instance_uuid: str) -> tuple:
        instance = self.folio_client.folio_get(f"/inventory/instances/{instance_uuid}")
        version = instance["_version"]
        hrid = instance["hrid"]
        return version, hrid

    def __put_folio_record__(self, instance_uuid: str, record: Record) -> bool:
        """
        Updates FOLIO SRS with updated MARC record with new OCLC Number
        in the 035 field
        """
        marc_json = record.as_json()
        version, instance_hrid = self.__instance_info__(instance_uuid)
        srs_uuid = self.__get_srs_record_id__(instance_uuid)
        if srs_uuid is None:
            logger.error(
                f"Failed to retrieve Active SRS uuid for Instance {instance_uuid}"
            )
            return False
        put_result = self.httpx_client.put(
            f"{self.folio_client.okapi_url}/change-manager/parsedRecords/{srs_uuid}",
            headers=self.folio_client.okapi_headers,
            json={
                "id": srs_uuid,
                "recordType": "MARC_BIB",
                "relatedRecordVersion": version,
                "parsedRecord": {"content": marc_json},
                "externalIdsHolder": {
                    "instanceId": instance_uuid,
                    "instanceHrid": instance_hrid,
                },
            },
        )
        if put_result.status_code != 202:
            logger.error(
                f"Failed to update FOLIO for Instance {instance_uuid} with SRS {srs_uuid}"
            )
            return False
        return True

    def __read_marc_files__(self, marc_files: list) -> list:
        records = []
        for marc_file in marc_files:
            marc_file_path = pathlib.Path(marc_file)
            if marc_file_path.exists():
                with marc_file_path.open('rb') as fo:
                    marc_reader = pymarc.MARCReader(fo)
                    records.extend([(r, str(marc_file_path)) for r in marc_reader])
        return records

    def __extract_control_number_035__(
        self, oclc_put_result: bytes
    ) -> Union[str, None]:
        """
        Extracts new OCLC number from 035 record
        """
        control_number = None
        oclc_record = pymarc.Record(data=oclc_put_result)  # type: ignore
        fields_035 = oclc_record.get_fields('035')
        for field in fields_035:
            for subfield in field.get_subfields("a"):
                matched_oclc = OCLC_REGEX.match(subfield)
                if matched_oclc:
                    _, control_number = matched_oclc.groups()
                    break
            if control_number:
                break
        return control_number

    def __update_oclc_number__(self, control_number: str, record: Record) -> Record:
        """
        Updates 035 field if control_number has changed or adds new 035 field if control_number
        is not found
        """
        needs_new_035 = True
        for field in record.get_fields('035'):
            for i, subfield in enumerate(field.subfields):
                if subfield.code == "a":
                    matched_oclc = OCLC_REGEX.match(subfield.value)
                    if matched_oclc:
                        suffix, oclc_number = matched_oclc.groups()
                        # Test if control number already exists
                        if oclc_number == control_number:
                            if suffix is None:
                                # Change prefix to include -M
                                new_prefix = subfield.value.replace(
                                    "(OCoLC)", "(OCoLC-M)"
                                )
                                field.subfields.pop(i)
                                field.add_subfield(code="a", value=new_prefix, pos=i)
                            needs_new_035 = False
                            break
        if needs_new_035:
            new_035 = pymarc.Field(
                tag='035',
                indicators=[' ', ' '],  # type: ignore
                subfields=[
                    pymarc.Subfield(code='a', value=f"(OCoLC-M){control_number}")
                ],
            )
            record.add_ordered_field(new_035)
        return record

    def __oclc_operations__(self, **kwargs) -> dict:
        marc_files: List[str] = kwargs['marc_files']
        function: Callable = kwargs['function']
        no_recs_message: str = kwargs.get("no_recs_message", "")
        output: dict = {"success": [], "failures": []}

        if len(marc_files) < 1:
            logger.info(no_recs_message)
            return output

        marc_records = self.__read_marc_files__(marc_files)

        successful_files: set = set()
        failed_files: set = set()

        with MetadataSession(authorization=self.oclc_token, timeout=30) as session:
            for record, file_name in marc_records:
                instance_uuid = get_instance_uuid(record)
                if instance_uuid is None:
                    continue
                try:
                    function(
                        session=session,
                        output=output,
                        record=record,
                        file_name=file_name,
                        instance_uuid=instance_uuid,
                        successes=successful_files,
                        failures=failed_files,
                    )
                except WorldcatRequestError as e:
                    msg = f"Instance UUID {instance_uuid} Error: {e}"
                    logger.error(msg)
                    output['failures'].append(
                        {
                            "uuid": instance_uuid,
                            "reason": "WorldcatRequest Error",
                            "context": str(e),
                        }
                    )
                    failed_files.add(file_name)
                    continue
        output["archive"] = list(successful_files.difference(failed_files))
        return output

    def __test_oclc_numbers__(self, oclc_numbers: list, instance_uuid: str):
        error_payload = None
        if len(oclc_numbers) != 1:
            match len(oclc_numbers):

                case 0:
                    msg = "Missing OCLC number"
                    error_payload = {
                        "uuid": instance_uuid,
                        "reason": msg,
                        "context": None,
                    }

                case _:
                    msg = "Multiple OCLC ids"
                    error_payload = {
                        "uuid": instance_uuid,
                        "reason": msg,
                        "context": sorted(oclc_numbers),  # type: ignore
                    }

            logger.error(msg)

        return error_payload

    def delete(self, marc_files: List[str]) -> dict:

        def __delete_oclc__(**kwargs):
            session: MetadataSession = kwargs["session"]
            output: dict = kwargs["output"]
            record: pymarc.Record = kwargs["record"]
            instance_uuid: str = kwargs["instance_uuid"]
            file_name: str = kwargs["file_name"]
            successes: set = kwargs["successes"]
            failures: set = kwargs["failures"]

            oclc_id = get_record_id(record)

            error_payload = self.__test_oclc_numbers__(oclc_id, instance_uuid)

            if error_payload:
                output['failures'].append(error_payload)
                failures.add(file_name)
                return

            response = session.holdings_unset(oclcNumber=oclc_id[0])
            if response:
                response = response.json()

            if response and response['success']:
                logger.info(f"Matched {instance_uuid} result {response}")
                output['success'].append(instance_uuid)
                successes.add(file_name)
            else:
                msg = "Failed holdings_unset"
                logger.info(f"{msg} for {instance_uuid} OCLC response: {response}")
                output['failures'].append(
                    {"uuid": instance_uuid, "reason": msg, "context": response}
                )
                failures.add(file_name)

        output = self.__oclc_operations__(
            marc_files=marc_files,
            function=__delete_oclc__,
            no_recs_message="No marc records for deletes",
        )
        return output

    def match(self, marc_files: List[str]) -> dict:

        def __match_oclc__(**kwargs):
            session: MetadataSession = kwargs["session"]
            output: dict = kwargs["output"]
            record: pymarc.Record = kwargs["record"]
            instance_uuid: str = kwargs["instance_uuid"]
            file_name: str = kwargs["file_name"]
            failures: set = kwargs["failures"]
            successes: set = kwargs["successes"]

            export_record = copy.deepcopy(record)
            export_record.remove_fields(*oclc_excluded)
            marc21 = export_record.as_marc21()

            matched_record_result = session.bib_match(
                record=marc21,
                recordFormat="application/marc",
            )
            logger.info(f"Matched Record Result {matched_record_result.json()}")
            matched_record = matched_record_result.json()
            if matched_record['numberOfRecords'] < 1:
                output['failures'].append(
                    {
                        "uuid": instance_uuid,
                        "reason": "Match failed",
                        "context": matched_record,
                    }
                )
                failures.add(file_name)
                return

            # Use first brief record's oclcNumber to add to existing MARC
            # record
            control_number = matched_record['briefRecords'][0]['oclcNumber']

            modified_marc_record = self.__update_oclc_number__(control_number, record)

            if self.__put_folio_record__(instance_uuid, modified_marc_record):

                # Sets holdings using the OCLC number
                update_holding_result = session.holdings_set(oclcNumber=control_number)

                if update_holding_result is not None:
                    update_holding_result = update_holding_result.json()

                if update_holding_result and update_holding_result['success']:
                    logger.info(
                        f"Sets new holdings for {instance_uuid} OCLC {update_holding_result}"
                    )
                    output['success'].append(instance_uuid)
                    successes.add(file_name)
                    return

                output['failures'].append(
                    {
                        "uuid": instance_uuid,
                        "reason": "Failed to update holdings after match",
                        "context": update_holding_result,
                    }
                )
                failures.add(file_name)
            else:
                output['failures'].append(
                    {
                        "uuid": instance_uuid,
                        "reason": "FOLIO failed to Add OCLC number",
                        "context": control_number,
                    }
                )
                failures.add(file_name)

        output = self.__oclc_operations__(
            marc_files=marc_files,
            function=__match_oclc__,
            no_recs_message="No new marc records",
        )
        return output

    def new(self, marc_files: dict) -> dict:

        def __add_update_control_number__(**kwargs):
            session: MetadataSession = kwargs["session"]
            control_number: str = kwargs["control_number"]
            record: pymarc.Record = kwargs["record"]
            output: dict = kwargs["output"]
            instance_uuid: str = kwargs["instance_uuid"]
            file_name: str = kwargs["file_name"]
            failures: set = kwargs["failures"]

            modified_marc_record = self.__update_oclc_number__(control_number, record)
            successful_add = False

            if self.__put_folio_record__(instance_uuid, modified_marc_record):
                # Sets holdings using the OCLC number
                new_holding_result = session.holdings_set(oclcNumber=control_number)
                if new_holding_result:
                    payload = new_holding_result.json()
                    if payload['success']:
                        logger.info(
                            f"Sets new holdings for {instance_uuid} OCLC {payload}"
                        )
                        output['success'].append(instance_uuid)
                        successful_add = True
                    else:
                        logger.error(
                            f"OCLC holdings_set call failed for {instance_uuid} OCLC {payload}"
                        )
                        output['failures'].append(
                            {
                                "uuid": instance_uuid,
                                "reason": "Failed to update holdings for new record",
                                "context": payload,
                            }
                        )
                        failures.add(file_name)
                else:
                    output['failures'].append(
                        {
                            "uuid": instance_uuid,
                            "reason": "No response from OCLC",
                            "context": None,
                        }
                    )
                    failures.add(file_name)
                    logger.error(
                        f"No response from OCLC holdings_set for {instance_uuid}"
                    )
            return successful_add

        def __new_oclc__(**kwargs):
            session: MetadataSession = kwargs["session"]
            output: dict = kwargs["output"]
            record: pymarc.Record = kwargs["record"]
            instance_uuid: str = kwargs["instance_uuid"]
            file_name: str = kwargs["file_name"]
            successes: set = kwargs["successes"]
            failures: set = kwargs["failures"]

            export_record = copy.deepcopy(record)
            export_record.remove_fields(*oclc_excluded)

            marc21 = export_record.as_marc21()

            # We want to capture errors from the OCLC response instead of
            # trying to parse the WorldcatRequestError
            bib_create_result = self.httpx_client.post(
                session._url_manage_bibs_create(),
                headers={
                    "Accept": "application/marc",
                    "content-type": "application/marc",
                    "Authorization": f"Bearer {session.authorization.token_str}",
                },
                data=marc21,
            )

            logger.info(
                f"New record result {bib_create_result.status_code} {bib_create_result.content}"
            )
            match bib_create_result.status_code:

                case 200:
                    control_number = self.__extract_control_number_035__(
                        bib_create_result.content
                    )
                    if control_number is None:
                        output['failures'].append(
                            {
                                "uuid": instance_uuid,
                                "reason": "Failed to extract OCLC number",
                                "context": None,
                            }
                        )
                        failures.add(file_name)
                        return

                case _:
                    try:
                        context = bib_create_result.json()
                    except json.decoder.JSONDecodeError:
                        context = bib_create_result.text

                    output['failures'].append(
                        {
                            "uuid": instance_uuid,
                            "reason": "Failed to add new MARC record",
                            "context": context,
                        }
                    )
                    failures.add(file_name)
                    return

            if __add_update_control_number__(
                session=session,
                instance_uuid=instance_uuid,
                control_number=control_number,
                output=output,
                record=record,
                successes=successes,
                failures=failures,
                file_name=file_name,
            ):
                output['success'].append(instance_uuid)
                successes.add(file_name)
            else:
                output['failures'].append(
                    {
                        "uuid": instance_uuid,
                        "reason": "FOLIO failed to Add OCLC number",
                        "context": control_number,
                    }
                )

        output = self.__oclc_operations__(
            marc_files=marc_files,
            function=__new_oclc__,
            no_recs_message="No new marc records",
        )
        # De-dup any success uuids
        output['success'] = list(set(output['success']))
        return output

    def update(self, marc_files: List[str]):
        def __update_oclc__(**kwargs):
            session: MetadataSession = kwargs["session"]
            output: dict = kwargs["output"]
            record: pymarc.Record = kwargs["record"]
            instance_uuid: str = kwargs["instance_uuid"]
            file_name: str = kwargs["file_name"]
            successes: set = kwargs["successes"]
            failures: set = kwargs["failures"]

            oclc_id = get_record_id(record)
            error_payload = self.__test_oclc_numbers__(oclc_id, instance_uuid)
            if error_payload:
                output['failures'].append(error_payload)
                failures.add(file_name)
                return

            response = session.holdings_set(oclcNumber=oclc_id[0])

            if response is None:
                output['failures'].append(
                    {
                        "uuid": instance_uuid,
                        "reason": "No response from OCLC",
                        "context": None,
                    }
                )
                failures.add(file_name)
                logger.error(f"No response from OCLC holdings_set for {instance_uuid}")
                return

            set_payload = response.json()
            logger.info(
                f"OCLC result from holdings_set for {instance_uuid}:\n{set_payload}"
            )
            if not set_payload['success']:
                output["failures"].append(
                    {
                        "uuid": instance_uuid,
                        "reason": "Failed to update holdings",
                        "context": set_payload,
                    }
                )
                failures.add(file_name)
                return

            control_number = set_payload['controlNumber']

            modified_marc_record = self.__update_oclc_number__(control_number, record)
            if self.__put_folio_record__(instance_uuid, modified_marc_record):
                output['success'].append(instance_uuid)
                successes.add(file_name)
            else:
                output['failures'].append(
                    {
                        "uuid": instance_uuid,
                        "reason": "FOLIO failed to Add OCLC number",
                        "context": control_number,
                    }
                )
                failures.add(file_name)

        output = self.__oclc_operations__(
            marc_files=marc_files,
            function=__update_oclc__,
            no_recs_message="No updated marc records",
        )
        return output
