import copy
import logging
import pathlib

import pymarc

from airflow.models import Variable
from folioclient import FolioClient

logger = logging.getLogger(__name__)

excluded_tags = [
    '592',
    '594',
    '596',
    '597',
    '598',
    '599',
    '695',
    '699',
    '790',
    '791',
    '792',
    '793',
    '850',
    '851',
    '853',
    '854',
    '855',
    '863',
    '864',
    '866',
    '871',
    '890',
    '891',
    '897',
    '898',
    '899',
    '905',
    '909',
    '911',
    '920',
    '922',
    '925',
    '926',
    '927',
    '928',
    '930',
    '933',
    '934',
    '935',
    '936',
    '937',
    '942',
    '943',
    '944',
    '945',
    '946',
    '947',
    '948',
    '949',
    '950',
    '951',
    '952',
    '954',
    '955',
    '957',
    '959',
    '960',
    '961',
    '962',
    '963',
    '965',
    '966',
    '967',
    '971',
    '975',
    '980',
    '983',
    '984',
    '985',
    '986',
    '987',
    '988',
    '990',
    '996',
]


def _add_holdings_items_fields(
    instance_subfields: list, lookups: dict, folio_client: FolioClient
) -> list:
    fields = []
    for uuid in instance_subfields:
        holdings_result = folio_client.folio_get(
            f"/holdings-storage/holdings?query=(instanceId=={uuid})"
        )
        for holding in holdings_result['holdingsRecords']:
            field_999 = _add_holdings_subfields(holding, lookups)

            items_result = folio_client.folio_get(
                f"inventory/items?query=(holdingsRecordId=={holding['id']})"
            )
            items = items_result['items']
            match len(items):
                case 0:
                    if len(field_999.subfields) > 0:
                        fields.append(field_999)

                case 1:
                    _add_item_subfields(field_999, items[0], lookups)
                    if len(field_999.subfields) > 0:
                        fields.append(field_999)

                case _:
                    org_999 = copy.deepcopy(field_999)
                    _add_item_subfields(field_999, items[0], lookups)
                    if len(field_999.subfields) > 0:
                        fields.append(field_999)
                    for item in items[1:]:
                        new_999 = copy.deepcopy(org_999)
                        _add_item_subfields(new_999, item, lookups)
                        if len(new_999.subfields) > 0:
                            fields.append(new_999)
    return fields


def _add_holdings_subfields(holdings: dict, lookups: dict) -> pymarc.Field:
    field_999 = pymarc.Field(tag="999", indicators=[' ', ' '])
    if len(holdings["holdingsTypeId"]) > 0:
        holdings_type_name = lookups["holdings_type"].get(holdings["holdingsTypeId"])
        if holdings_type_name:
            field_999.add_subfield('h', holdings_type_name)
    if len(holdings['permanentLocationId']) > 0:
        permanent_location_code = lookups['locations'].get(
            holdings['permanentLocationId']
        )
        if permanent_location_code:
            field_999.add_subfield('l', permanent_location_code)
    if len(holdings['callNumberTypeId']) > 0:
        call_number_type = lookups["call_number"].get(holdings['callNumberTypeId'])
        if call_number_type:
            field_999.add_subfield('w', call_number_type)
    if len(holdings['callNumber']) > 0:
        field_999.add_subfield('a', holdings['callNumber'], 0)
    return field_999


def _add_item_subfields(field_999: pymarc.Field, item: dict, lookups: dict):
    if 'materialType' in item:
        field_999.add_subfield('t', item['materialType'].get('name'))
    if 'effectiveLocation' in item:
        location_code = lookups['locations'].get(item['effectiveLocation'].get('id'))
        if location_code:
            field_999.add_subfield('e', location_code)
    if len(item.get('numberOfPieces', '')) > 0:
        field_999.add_subfield('j', item['numberOfPieces'])

    if "enumeration" in item:
        for subfield in field_999.subfields:
            if subfield.code == 'a':
                value = field_999.delete_subfield('a')
                value = f"{value} {item['enumeration']}"
                field_999.add_subfield('a', value, 0)


def _call_number_lookup(folio_client: FolioClient) -> dict:
    lookup = {}
    for row in folio_client.call_number_types:
        lookup[row['id']] = row['name']
    return lookup


def _holdings_type_lookup(folio_client: FolioClient) -> dict:
    lookup = {}
    for row in folio_client.holdings_types:
        lookup[row['id']] = row['name']
    return lookup


def _locations_lookup(folio_client: FolioClient) -> dict:
    lookup = {}
    for location in folio_client.locations:
        lookup[location['id']] = location['code']
    return lookup


def add_holdings_items(marc_file: str, folio_client: FolioClient = None):
    """
    Adds FOLIO Holdings and Items information to
    """
    if folio_client is None:
        folio_client = FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )

    look_ups = {
        "call_number": _call_number_lookup(folio_client),
        "holdings_type": _holdings_type_lookup(folio_client),
        "locations": _locations_lookup(folio_client),
    }

    marc_path = pathlib.Path(marc_file)

    marc_records = []
    logger.info("Starting MARC processing")
    with marc_path.open('rb') as fo:
        for i, record in enumerate(pymarc.MARCReader(fo)):
            subfields_i = record["999"].get_subfields("i")
            new_999s = _add_holdings_items_fields(subfields_i, look_ups, folio_client)
            record.add_field(*new_999s)
            marc_records.append(record)
            if not i % 100:
                logger.info(f"{i:,} processed records")

    logger.info(f"Writing {len(marc_records):,} modified MARC records to {marc_path}")
    with marc_path.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in marc_records:
            marc_writer.write(record)


def remove_marc_fields(marc_file: str):
    """
    Removes MARC fields from export MARC21 file
    """
    marc_path = pathlib.Path(marc_file)

    with marc_path.open('rb') as fo:
        marc_records = [record for record in pymarc.MARCReader(fo)]

    logger.info(f"Removing MARC fields for {len(marc_records):,} records")

    for i, record in enumerate(marc_records):
        record.remove_fields(*excluded_tags)
        if not i % 100:
            logger.info(f"{i:,} records processed")

    with marc_path.open("wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in marc_records:
            marc_writer.write(record)
