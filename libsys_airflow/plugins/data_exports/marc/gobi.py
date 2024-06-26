import itertools
import logging
import pathlib
import pymarc
import re

from libsys_airflow.plugins.data_exports.marc.transformer import Transformer

logger = logging.getLogger(__name__)


def gobi_list_from_marc_files(marc_file_list: dict):
    gobi_lists = []
    gobi_transformer = GobiTransformer()
    for file in marc_file_list['new']:
        gobi_lists.append(gobi_transformer.generate_list(marc_file=file))

    return gobi_lists


class GobiTransformer(Transformer):
    def generate_list(self, marc_file):

        # marc_path is data-export-files/gobi/marc-files/updates/YYYYMMDD.mrc
        marc_path = pathlib.Path(marc_file)
        gobi_list_name = marc_path.stem
        # gobi_path is data-export-files/gobi/marc-files/updates/YYYMMDD.txt
        gobi_path = pathlib.Path(marc_path.parent) / f"{gobi_list_name}.txt"

        with marc_path.open('rb') as fo:
            marc_records = [record for record in pymarc.MARCReader(fo)]

            logger.info(
                f"Reading {len(marc_records):,} record(s) for gobi data export list"
            )

            print_list = []
            ebook_list = []

            for i, record in enumerate(marc_records):
                if not i % 100:
                    logger.info(f"{i:,} records processed")

                field856 = record.get_fields("856")
                field856x = [s.get_subfields("x") for s in field856]
                fields856x = list(itertools.chain.from_iterable(field856x))

                field956 = record.get_fields("956")
                field956x = [s.get_subfields("x") for s in field956]
                fields956x = list(itertools.chain.from_iterable(field956x))

                try:
                    instance_id = self.instance_subfields(record)[0]
                except IndexError:
                    continue

                holdings_result = self.folio_client.folio_get(
                    f"/holdings-storage/holdings?query=(instanceId=={instance_id})"
                )

                stdnums = []
                isbns = record.get_fields("020")
                for stdnum in isbns:
                    try:
                        isbn = stdnum.get_subfields("a")[0]
                        """
                        Require 10 or 13 digits, except 10 digits can have "X" as last "digit".
                        Some ISBNs are written with embedded hyphens to ignore, or additional text after a space.
                        """
                        isbn = isbn.replace("-", "")
                        isbn = re.sub(r"\s.*", "", isbn)
                        if self.isbn_regex.match(isbn):
                            stdnums.append(isbn)

                    except IndexError:
                        continue

                for holding in holdings_result['holdingsRecords']:
                    ebook = False

                    campus = self.campus_lookup.get(holding.get('permanentLocationId'))
                    if not campus == 'SUL':
                        continue

                    if len(holding.get("holdingsTypeId", "")) > 0:
                        if (
                            self.holdings_type.get(holding["holdingsTypeId"])
                            == 'Electronic'
                        ):
                            ebook = True

                            if set(['subscribed', 'gobi']).intersection(
                                set([s.lower() for s in fields856x + fields956x])
                            ):
                                ebook = False

                    if ebook:
                        ebook_list.extend(stdnums)

                    items_result = self.folio_client.folio_get(
                        f"/item-storage/items?query=(holdingsRecordId=={holding['id']})"
                    )

                    if len(items_result['items']):
                        print_list.extend(stdnums)

        with gobi_path.open("w+") as (fo):
            for p_isbn in print_list:
                fo.write(f"{p_isbn}|print|325099\n")

            for e_isbn in ebook_list:
                fo.write(f"{e_isbn}|ebook|325099\n")
