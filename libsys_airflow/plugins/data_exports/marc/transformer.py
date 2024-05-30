import copy
import logging
import pathlib
import pymarc
import re

from libsys_airflow.plugins.folio_client import folio_client
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from s3path import S3Path

logger = logging.getLogger(__name__)


class Transformer(object):
    def __init__(self):
        self.folio_client = folio_client()
        self.call_numbers = self.call_number_lookup()
        self.holdings_type = self.holdings_type_lookup()
        self.locations = self.locations_lookup()
        self.materialtypes = self.materialtype_lookup()
        self.campus_lookup = self.campus_by_locations_lookup()
        self.uuid_regex = self.uuid_compile()
        self.isbn_regex = self.isbn_compile()

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

    def materialtype_lookup(self) -> dict:
        lookup = {}
        materialtypes = self.folio_client.folio_get("/material-types?limit=99")[
            "mtypes"
        ]
        for m in materialtypes:
            lookup[m['id']] = m['name']
        return lookup

    def campus_by_locations_lookup(self) -> dict:
        lookup = {}
        campus_result = self.folio_client.folio_get("/location-units/campuses")
        campus_lookup = {}
        for campus in campus_result.get("loccamps"):
            campus_lookup[campus['id']] = campus["code"]
        for location in self.folio_client.locations:
            lookup[location['id']] = campus_lookup.get(location['campusId'])
        return lookup

    def uuid_compile(self) -> re.Pattern:
        return re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        )

    def isbn_compile(self) -> re.Pattern:
        return re.compile(r"^(?=(?:\d){9}[\dX](?:(?:\D*\d){3})?$)(?:[\dX]{10}|\d{13})$")

    def instance_subfields(self, record):
        subfields_i = []
        fields_999 = record.get_fields("999")

        for field_999 in fields_999:
            uuids = field_999.get_subfields("i")
            for uuid in uuids:
                if self.uuid_regex.match(uuid):
                    subfields_i.append(uuid)

        return subfields_i

    def add_holdings_items(self, marc_file: str, full_dump: bool):
        """
        Adds FOLIO Holdings and Items information to MARC records
        """

        marc_path = pathlib.Path(marc_file)
        if full_dump:
            marc_path = S3Path(marc_file)
            logger.info("Adding holdings and items using AWS S3 with path")

        marc_records = []
        logger.info(f"Starting MARC processing on {marc_path}")

        with marc_path.open('rb') as fo:
            for i, record in enumerate(pymarc.MARCReader(fo)):
                try:
                    subfields_i = self.instance_subfields(record)

                    if subfields_i:
                        new_999s = self.add_holdings_items_fields(subfields_i)
                        record.add_field(*new_999s)
                        marc_records.append(record)

                    if not i % 100:
                        logger.info(f"{i:,} processed records")
                except Exception as e:
                    logger.warning(e)
                    continue

        logger.info(
            f"Writing {len(marc_records):,} modified MARC records to {marc_path}"
        )
        with marc_path.open("wb") as fo:
            marc_writer = pymarc.MARCWriter(fo)
            for record in marc_records:
                marc_writer.write(record)

    def add_holdings_items_fields(self, instance_subfields: list) -> list:
        fields = []
        for uuid in instance_subfields:
            try:
                holdings_result = SQLExecuteQueryOperator(
                    task_id="query-holdings",
                    conn_id="postgres_folio",
                    database="okapi",
                    sql=f"select jsonb from sul_mod_inventory_storage.holdings_record where instanceid = '{uuid}'",
                ).execute(get_current_context())

                for holding_tuple in holdings_result:
                    holding = holding_tuple[0]

                    if holding.get("discoverySuppress", False):
                        continue

                    field_999 = self.add_holdings_subfields(holding)

                    holding_id = holding["id"]

                    items_result = SQLExecuteQueryOperator(
                        task_id="query-items",
                        conn_id="postgres_folio",
                        database="okapi",
                        sql=f"select jsonb from sul_mod_inventory_storage.item where holdingsrecordid = '{holding_id}'",
                    ).execute(get_current_context())

                    active_items = []
                    for _item in items_result:
                        if not _item[0].get("discoverySuppress", False):
                            active_items.append(_item)

                    match len(active_items):
                        case 0:
                            if len(field_999.subfields) > 0:
                                fields.append(field_999)

                        case 1:
                            self.add_item_subfields(field_999, active_items[0][0])
                            if len(field_999.subfields) > 0:
                                fields.append(field_999)

                        case _:
                            org_999 = copy.deepcopy(field_999)
                            self.add_item_subfields(field_999, active_items[0][0])
                            if len(field_999.subfields) > 0:
                                fields.append(field_999)
                            for item_tuple in active_items[1:]:
                                item = item_tuple[0]
                                new_999 = copy.deepcopy(org_999)
                                self.add_item_subfields(new_999, item)
                                if len(new_999.subfields) > 0:
                                    fields.append(new_999)
            except Exception as e:
                logger.warning(f"Error with holdings or items: {e}")
                continue

        return fields

    def add_holdings_subfields(self, holding: dict) -> pymarc.Field:
        field_999 = pymarc.Field(tag="999", indicators=[' ', ' '])
        if len(holding.get("holdingsTypeId", "")) > 0:
            holdings_type_name = self.holdings_type.get(holding["holdingsTypeId"])
            if holdings_type_name:
                field_999.add_subfield('h', holdings_type_name)
        if len(holding.get("permanentLocationId", "")) > 0:
            permanent_location_code = self.locations.get(holding['permanentLocationId'])
            if permanent_location_code:
                field_999.add_subfield('l', permanent_location_code)
        if len(holding.get("callNumberTypeId", "")) > 0:
            call_number_type = self.call_numbers.get(holding['callNumberTypeId'])
            if call_number_type:
                field_999.add_subfield('w', call_number_type)
        if len(holding.get("callNumber", "")) > 0:
            field_999.add_subfield('a', holding['callNumber'], 0)
        return field_999

    def add_item_subfields(self, field_999: pymarc.Field, item: dict):
        if 'barcode' in item:
            field_999.add_subfield('i', item['barcode'])
        if 'materialTypeId' in item:
            field_999.add_subfield('t', self.materialtypes.get(item['materialTypeId']))
        if 'effectiveLocationId' in item:
            location_code = self.locations.get(item['effectiveLocationId'])
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
