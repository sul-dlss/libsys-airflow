import ast
import itertools
import logging
import pathlib
import pymarc
import re

from libsys_airflow.plugins.data_exports.marc.transformer import Transformer

logger = logging.getLogger(__name__)


def gobi_list_from_marc_files(marc_file_list: str):
    gobi_lists = []
    gobi_transformer = GobiTransformer()
    for marc_file in ast.literal_eval(marc_file_list):
        gobi_lists.append(gobi_transformer.generate_list(marc_file=marc_file))

    return gobi_lists


class GobiTransformer(Transformer):
    def generate_list(self, marc_file) -> pathlib.Path:
        marc_path = pathlib.Path(marc_file)
        gobi_list_name = marc_path.stem
        gobi_path = pathlib.Path(marc_path.parent.parent) / f"stf.{gobi_list_name}.txt"

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

                isbns = record.get_fields("020")
                for stdnum in isbns:
                    reisbn = stdnum.get_subfields("a")[0]

                    """
                    Require 10 or 13 digits, except 10 digits can have "X" as last "digit".
                    Some ISBNs are written with embedded hyphens to ignore, or additional text after a space.
                    """
                    reisbn.replace("-", "")
                    isbn = re.sub("\s.*", "", reisbn)
                    if not re.search(
                        r"^(?=(?:\D*\d){10}(?:(?:\D*\d){3})?$)[\d]+$", isbn
                    ):
                        break

                    fields035 = record.get_fields("035")
                    field856 = record.get_fields("856")
                    field856x = [s.get_subfields("x") for s in field856]
                    fields856x = list(itertools.chain.from_iterable(field856x))

                    field956 = record.get_fields("956")
                    field956x = [s.get_subfields("x") for s in field956]
                    fields956x = list(itertools.chain.from_iterable(field956x))

                    holdings_result = self.folio_client.folio_get(
                        f"/holdings-storage/holdings?query=(isbn=={isbn})"
                    )

                    for holding in holdings_result['holdingsRecords']:
                        ebook = False

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

                        for field035 in fields035:
                            if re.search("^gls[0-9]+", field035.get_subfields("a")[0]):
                                ebook = False

                        if ebook:
                            ebook_list.append(isbn)

                        items_result = self.folio_client.folio_get(
                            f"inventory/items?query=(holdingsRecordId=={holding['id']})"
                        )

                        if len(items_result['items']):
                            print_list.append(isbn)
                            

        with gobi_path.open("w+") as (fo):
            for p_isbn in print_list:
                fo.write(f"{p_isbn}|print|325099\n")

            for e_isbn in ebook_list:
                fo.write(f"{e_isbn}|ebook|325099\n")

        return gobi_path
