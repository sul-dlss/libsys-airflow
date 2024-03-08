import copy
import logging
import pathlib
import pymarc

from libsys_airflow.plugins.folio_client import folio_client

logger = logging.getLogger(__name__)

class Transformer(object):
  def __init__(self):
    self.folio_client = folio_client()
    self.call_numbers = self.call_number_lookup()
    self.holdings_type = self.holdings_type_lookup()
    self.locations = self.locations_lookup()


  def call_number_lookup(self) -> dict:
    lookup = {}
    for row in self.folio_client.call_number_types:
        lookup[row['id']] = row['name']
    return lookup


  def holdings_type_lookup(self) -> dict:
    lookup = {}
    for row in self.folio_client.holdings_types:
        lookup[row['id']] = row['name']
    return lookup


  def locations_lookup(self) -> dict:
    lookup = {}
    for location in self.folio_client.locations:
        lookup[location['id']] = location['code']
    return lookup


  def add_holdings_items(self, marc_file: str):
    """
    Adds FOLIO Holdings and Items information to MARC records
    """

    marc_path = pathlib.Path(marc_file)

    marc_records = []
    logger.info("Starting MARC processing")
    with marc_path.open('rb') as fo:
        for i, record in enumerate(pymarc.MARCReader(fo)):
            subfields_i = record["999"].get_subfields("i")
            new_999s = self.add_holdings_items_fields(subfields_i)
            record.add_field(*new_999s)
            marc_records.append(record)
            if not i % 100:
                logger.info(f"{i:,} processed records")

    logger.info(f"Writing {len(marc_records):,} modified MARC records to {marc_path}")
    with marc_path.open('wb+') as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for record in marc_records:
            marc_writer.write(record)


  def add_holdings_items_fields(self, instance_subfields: list) -> list:
    fields = []
    for uuid in instance_subfields:
        holdings_result = self.folio_client.folio_get(
            f"/holdings-storage/holdings?query=(instanceId=={uuid})"
        )
        for holding in holdings_result['holdingsRecords']:
            field_999 = self.add_holdings_subfields(holding)

            items_result = self.folio_client.folio_get(
                f"inventory/items?query=(holdingsRecordId=={holding['id']})"
            )
            items = items_result['items']
            match len(items):
                case 0:
                    if len(field_999.subfields) > 0:
                        fields.append(field_999)

                case 1:
                    self.add_item_subfields(field_999, items[0])
                    if len(field_999.subfields) > 0:
                        fields.append(field_999)

                case _:
                    org_999 = copy.deepcopy(field_999)
                    self.add_item_subfields(field_999, items[0])
                    if len(field_999.subfields) > 0:
                        fields.append(field_999)
                    for item in items[1:]:
                        new_999 = copy.deepcopy(org_999)
                        self.add_item_subfields(new_999, item)
                        if len(new_999.subfields) > 0:
                            fields.append(new_999)
    return fields


  def add_holdings_subfields(self, holdings: dict) -> pymarc.Field:
    field_999 = pymarc.Field(tag="999", indicators=[' ', ' '])
    if len(holdings["holdingsTypeId"]) > 0:
        holdings_type_name = self.holdings_type.get(holdings["holdingsTypeId"])
        if holdings_type_name:
            field_999.add_subfield('h', holdings_type_name)
    if len(holdings['permanentLocationId']) > 0:
        permanent_location_code = self.locations.get(
            holdings['permanentLocationId']
        )
        if permanent_location_code:
            field_999.add_subfield('l', permanent_location_code)
    if len(holdings['callNumberTypeId']) > 0:
        call_number_type = self.call_numbers.get(holdings['callNumberTypeId'])
        if call_number_type:
            field_999.add_subfield('w', call_number_type)
    if len(holdings['callNumber']) > 0:
        field_999.add_subfield('a', holdings['callNumber'], 0)
    return field_999


  def add_item_subfields(self, field_999: pymarc.Field, item: dict):
    if 'materialType' in item:
        field_999.add_subfield('t', item['materialType'].get('name'))
    if 'effectiveLocation' in item:
        location_code = self.locations.get(item['effectiveLocation'].get('id'))
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