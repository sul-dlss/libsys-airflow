import httpx
import json
import logging
import pymarc

from typing import Union
from airflow.models import Variable

from libsys_airflow.plugins.shared.folio_client import folio_client

logger = logging.getLogger(__name__)


def is_production():
    return bool(Variable.get("OKAPI_URL").find("prod") > 0)


class FolioAddMarcTags(object):
    def __init__(self, **kwargs):
        self.httpx_client = httpx.Client()
        self.folio_client = folio_client()

    def put_folio_records(self, marc_instances_tags: dict, instance_id: str) -> bool:
        try:
            srs_record = self.__get_srs_record__(instance_id)
            srs_uuid = srs_record["recordId"]  # type: ignore
            marc_json = srs_record["parsedRecord"]["content"]  # type: ignore
            version, instance_hrid = self.__instance_info__(instance_id)
        except TypeError:
            logger.error(
                f"Failed to retrieve Active SRS uuid for Instance {instance_id}"
            )
            return False
        
        put_result = self.httpx_client.put(
            f"{self.folio_client.okapi_url}/change-manager/parsedRecords/{srs_uuid}",
            headers=self.folio_client.okapi_headers,
            json={
                "id": srs_uuid,
                "recordType": "MARC_BIB",
                "relatedRecordVersion": version,
                "parsedRecord": {
                    "content": self.__marc_json_with_new_tags__(
                        marc_json, marc_instances_tags
                    )
                },
                "externalIdsHolder": {
                    "instanceId": instance_id,
                    "instanceHrid": instance_hrid,
                },
            },
        )
        if put_result.status_code != 202:
            logger.error(
                f"Failed to update FOLIO for Instance {instance_id} with SRS {srs_uuid}"
            )
            return False
        return True

    def __marc_json_with_new_tags__(self, marc_json: dict, marc_instances_tags: dict):
        reader = pymarc.reader.JSONReader(json.dumps(marc_json))

        for tag_name, indicator_subfields in marc_instances_tags.items():
            for sfs in indicator_subfields['subfields']:
                for sf_code, sf_val in sfs.items():
                    new_tag = pymarc.Field(
                        tag=tag_name,
                        indicators=[indicator_subfields['ind1'], indicator_subfields['ind2']],  # type: ignore
                        subfields=[pymarc.Subfield(code=sf_code, value=sf_val)],
                    )
                    for record in reader:
                        existing_tags = record.get_fields(tag_name)
                        if self.__tag_is_unique__(existing_tags, new_tag):
                            record.add_field(new_tag)

        return record.as_json()

    def __get_srs_record__(self, instance_uuid: str) -> Union[dict, None]:
        source_storage_result = self.folio_client.folio_get(
            f"/source-storage/source-records?instanceId={instance_uuid}"
        )

        try:
            source_records = source_storage_result['sourceRecords']
            if len(source_records) < 1:
                logger.error(f"No Active SRS record found for {instance_uuid}")
                return None
            return source_records[0]

        except Exception as e:
            logger.error(
                f"Failed to retrieve Active SRS record for Instance {instance_uuid} error: {e}"
            )
            return None

    def __instance_info__(self, instance_uuid: str) -> tuple:
        instance = self.folio_client.folio_get(f"/inventory/instances/{instance_uuid}")
        version = instance["_version"]
        hrid = instance["hrid"]
        return version, hrid

    def __tag_is_unique__(self, fields: list, new_field: pymarc.Field) -> bool:
        for existing_fields in fields:
            for esubfield in existing_fields:
                for nsubfield in new_field:
                    if (
                        nsubfield.code == esubfield.code
                        and nsubfield.value == esubfield.value
                    ):
                        logger.info(f"Skip adding duplicated {new_field.tag} field")
                        return False
                    else:
                        return True
        return False
