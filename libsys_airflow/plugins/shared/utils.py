import httpx
import json
import logging
import pymarc
import re

from typing import Union
from airflow.models import Variable
from airflow.utils.email import send_email

from libsys_airflow.plugins.shared.folio_client import folio_client

logger = logging.getLogger(__name__)


def is_production():
    return bool(Variable.get("OKAPI_URL").find("prod") > 0)


def send_email_with_server_name(**kwargs):
    """
    send_email wrapper to include subject with server name
        when not run in production
    """
    devs_to_email_addr = Variable.get("EMAIL_DEVS")
    to_addresses = kwargs.get("to", devs_to_email_addr)
    subject = kwargs.get("subject")
    html_content = kwargs.get("html_content")
    send_email(
        to=to_addresses,
        subject=_subject_with_server_name(subject=subject),
        html_content=html_content,
    )


def _subject_with_server_name(**kwargs):
    subject = kwargs.get("subject")
    folio_url = Variable.get("FOLIO_URL", "folio-test/stage")
    if is_production():
        return subject
    else:
        folio_url = re.sub('https?://', '', folio_url)
        return f"{folio_url} - {subject}"


class FolioAddMarcTags(object):
    def __init__(self, **kwargs):
        self.httpx_client = httpx.Client()
        self.folio_client = folio_client()

    def put_folio_records(self, marc_instance_tags: dict, instance_id: str) -> bool:
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
                        marc_json, marc_instance_tags
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
        else:
            logger.info(
                f"Successfully updated FOLIO Instance {instance_id} with SRS {srs_uuid}"
            )
        return True

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

    def __marc_json_with_new_tags__(self, marc_json: dict, marc_instances_tags: dict):
        reader = pymarc.reader.JSONReader(json.dumps(marc_json))
        record = [record for record in reader][0]  # always one record in this context

        for tag_name, indicator_subfields in marc_instances_tags.items():
            logger.info(f"Constructing MARC tag {tag_name}")
            existing_tags = [
                str(field) for field in record.get_fields(tag_name)
            ]  # returns list of strings or empty if record doesn't have any
            if existing_tags:
                logger.info(
                    f"Record has existing {tag_name}'s. New fields will be evaluated for uniqueness."
                )
            else:
                logger.info(
                    f"Record does not have existing {tag_name}'s. New fields will be added."
                )
            # indicator_subfields:
            # [{'ind1': ' ', 'ind2': ' ', 'subfields': [{'f': 'STEINMETZ'}, ...]},
            # {'ind1': ' ', 'ind2': ' ', 'subfields': [{'f': 'WHITEHEAD'}, ...]}]
            new_tags = []
            for row in indicator_subfields:
                new_field = self.__construct_new_field__(row, tag_name)
                if self.__tag_is_unique__(existing_tags, new_field):
                    logger.info(f"New field {new_field.tag} is unique tag.")
                    new_tags.append(new_field)
                else:
                    logger.info(f"New field {new_field.tag} is not unique")

            for x in new_tags:
                record.add_ordered_field(x)

        record_json = record.as_json()
        logger.info(f"Constructing MARC record: {record_json}")
        return record_json

    def __construct_new_field__(
        self, indicator_subfields: dict, tag_name: str
    ) -> pymarc.Field:
        new_field = pymarc.Field(
            tag=tag_name, indicators=[indicator_subfields['ind1'], indicator_subfields['ind2']]  # type: ignore
        )
        for subfields in indicator_subfields['subfields']:
            self.__construct_new_subfields__(new_field, subfields)

        return new_field

    def __construct_new_subfields__(self, field: pymarc.Field, subfields: dict):
        for sf_code, sf_val in subfields.items():
            field.add_subfield(sf_code, sf_val)

        return field

    def __tag_is_unique__(self, fields: list, new_field: pymarc.Field) -> bool:
        new_field_string = str(new_field)
        if new_field_string in fields:
            logger.info(f"Skip adding duplicated {new_field_string} field")
            return False

        logger.info(f"{new_field_string} tag is unique")
        return True
