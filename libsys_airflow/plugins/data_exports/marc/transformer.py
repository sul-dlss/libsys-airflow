import copy
import logging
import pathlib
import pymarc

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

    def add_holdings_items(self, marc_file: str, full_dump: bool):
        """
        Adds FOLIO Holdings and Items information to MARC records
        """

        marc_path = pathlib.Path(marc_file)
        if full_dump:
            marc_path = S3Path(marc_file)
            logger.info(
                f"Adding holdings and items using AWS S3 with path: {marc_path}"
            )

        marc_records = []
        logger.info("Starting MARC processing")
        with marc_path.open('rb') as fo:
            for i, record in enumerate(pymarc.MARCReader(fo)):
                try:
                    subfields_i = record["999"].get_subfields("i")
                    new_999s = self.add_holdings_items_fields(subfields_i)
                    record.add_field(*new_999s)
                    marc_records.append(record)
                    if not i % 100:
                        logger.info(f"{i:,} processed records")
                except TypeError as e:
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
                    field_999 = self.add_holdings_subfields(holding)

                    holding_id = holding["id"]

                    items_result = SQLExecuteQueryOperator(
                        task_id="query-items",
                        conn_id="postgres_folio",
                        database="okapi",
                        sql=f"select jsonb from sul_mod_inventory_storage.item where holdingsrecordid = '{holding_id}'",
                    ).execute(get_current_context())

                    match len(items_result):
                        case 0:
                            if len(field_999.subfields) > 0:
                                fields.append(field_999)

                        case 1:
                            self.add_item_subfields(field_999, items_result[0][0])
                            if len(field_999.subfields) > 0:
                                fields.append(field_999)

                        case _:
                            org_999 = copy.deepcopy(field_999)
                            self.add_item_subfields(field_999, items_result[0][0])
                            if len(field_999.subfields) > 0:
                                fields.append(field_999)
                            for item_tuple in items_result[1:]:
                                item = item_tuple[0]
                                new_999 = copy.deepcopy(org_999)
                                self.add_item_subfields(new_999, item)
                                if len(new_999.subfields) > 0:
                                    fields.append(new_999)
            except Exception as e:
                logger.warning(f"Error with holdings or items: {e}")
                continue

        return fields

    def add_holdings_subfields(self, holdings: dict) -> pymarc.Field:
        field_999 = pymarc.Field(tag="999", indicators=[' ', ' '])
        if len(holdings.get("holdingsTypeId", "")) > 0:
            holdings_type_name = self.holdings_type.get(holdings["holdingsTypeId"])
            if holdings_type_name:
                field_999.add_subfield('h', holdings_type_name)
        if len(holdings.get("permanentLocationId", "")) > 0:
            permanent_location_code = self.locations.get(
                holdings['permanentLocationId']
            )
            if permanent_location_code:
                field_999.add_subfield('l', permanent_location_code)
        if len(holdings.get("callNumberTypeId", "")) > 0:
            call_number_type = self.call_numbers.get(holdings['callNumberTypeId'])
            if call_number_type:
                field_999.add_subfield('w', call_number_type)
        if len(holdings.get("callNumber", "")) > 0:
            field_999.add_subfield('a', holdings['callNumber'], 0)
        return field_999

    def add_item_subfields(self, field_999: pymarc.Field, item: dict):
        if 'materialTypeId' in item:
            field_999.add_subfield('t', item['materialTypeId'].get('name'))
        if 'effectiveLocationId' in item:
            location_code = self.locations.get(item['effectiveLocationId'].get('id'))
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
