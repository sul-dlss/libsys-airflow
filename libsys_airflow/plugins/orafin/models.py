from datetime import datetime

import numpy as np
import pandas as pd

from attrs import define
from typing import List, Union

from libsys_airflow.plugins.folio.folio_client import FolioClient
from libsys_airflow.plugins.folio.helpers.constants import expense_codes


@define
class Vendor:
    code: str
    erpCode: str
    id: str
    liableForVat: bool

    @property
    def vendor_number(self) -> str:
        return f"HD{self.erpCode}"


@define
class Fund:
    id: str
    externalAccountNo: str


@define
class fundDistribution:
    distributionType: str
    value: float

    fund: Union[Fund, None] = None


@define
class PurchaseOrderLine:
    id: str
    acquisitionMethod: str
    orderFormat: Union[str, None]
    materialType: Union[str, None]


@define
class InvoiceLine:
    adjustmentsTotal: float
    id: str
    invoiceLineNumber: str
    subTotal: float
    total: float

    expense_code: Union[str, None] = None
    poLine: Union[PurchaseOrderLine, None] = None
    fundDistributions: list[fundDistribution] = []

    @property
    def tax_exempt(self) -> bool:
        if self.adjustmentsTotal == 0.0:
            return True
        return False

    def tax_code(self, liable_for_vat: bool) -> str:
        if self.tax_exempt:
            return "TAX_EXEMPT"
        if liable_for_vat:
            return "SALES_STANDARD"
        return "USE_CA"

    def _generate_line(self, **kwargs) -> str:
        line_type: str = kwargs["line_type"]
        internal_number: str = kwargs["internal_number"]
        amount: float = kwargs["amount"]
        tax_code: str = kwargs["tax_code"]
        external_account_number: str = kwargs["external_account_number"]
        return "".join(
            [
                f"{internal_number: <13}",
                line_type,
                f"{amount:015.2f}",
                f"{tax_code: <20}",
                f"{external_account_number}",
                f"-{self.expense_code: <51}",
            ]
        )

    def generate_lines(self, internal_number: str, liable_for_vat: bool) -> list:
        output = []
        tax_code = self.tax_code(liable_for_vat)

        for fund_distribution in self.fundDistributions:
            rows = []
            if fund_distribution.distributionType.startswith('percentage'):
                percentage = fund_distribution.value / 100
                amount = self.subTotal * percentage
                adjusted_amt = self.adjustmentsTotal * percentage
            else:
                amount = fund_distribution.value
                adjusted_amt = self.adjustmentsTotal
            # Create DR line
            rows.append(
                self._generate_line(
                    line_type="DR",
                    internal_number=internal_number,
                    amount=amount,
                    tax_code=tax_code,
                    external_account_number=fund_distribution.fund.externalAccountNo,  # type: ignore
                )
            )
            # Create TX line if adjustmentsTotal not zero
            if self.adjustmentsTotal != 0:
                rows.append(
                    self._generate_line(
                        line_type="TX",
                        internal_number=internal_number,
                        amount=adjusted_amt,
                        tax_code=tax_code,
                        external_account_number=fund_distribution.fund.externalAccountNo,  # type: ignore
                    )
                )
                # Create TA line if not liable for VAT
                if liable_for_vat is False:
                    rows.append(
                        "".join(
                            [
                                f"{internal_number: <13}",
                                "TA",
                                f"{-adjusted_amt:015.2f}",
                                f"{tax_code: <20}",
                                f"{' ': <69}",
                            ]
                        )
                    )
            output.append({"rows": rows, "amount": amount})
        feeder_rows: List[str] = []
        for line in sorted(output, key=lambda x: x['amount'], reverse=True):  # type: ignore
            feeder_rows.extend(line["rows"])  # type: ignore
        return feeder_rows


@define
class Invoice:
    accountingCode: str
    id: str
    invoiceDate: datetime
    folioInvoiceNo: str
    subTotal: float
    vendorInvoiceNo: str
    total: float

    lines: list[InvoiceLine]
    vendor: Vendor
    paymentDue: Union[datetime, None] = None
    paymentTerms: Union[str, None] = None

    @property
    def amount(self):
        if self.vendor and self.vendor.liableForVat:
            return self.total
        return self.subTotal

    @property
    def attachment_flag(self):
        if self.paymentTerms and self.paymentTerms.upper().startswith("WILLCALL"):
            return "Y"
        return " "

    @property
    def internal_number(self):
        return f"LIB{self.folioInvoiceNo}"

    @property
    def invoice_type(self):
        if self.amount < 0:
            return "CR"
        return "DR"

    @property
    def terms_name(self):
        if self.paymentDue:
            return "IMMEDIATE"
        return "N30"

    def header(self):
        invoice_number = f"{self.vendorInvoiceNo} {self.folioInvoiceNo}"
        return "".join(
            [
                f"{self.internal_number: <13}",
                f"HD{self.accountingCode: <21}",
                f"{invoice_number: <40}",
                f"{self.invoiceDate.strftime('%Y%m%d')}",
                f"{self.amount:015.2f}",
                f"{self.invoice_type: <32}",
                f"{self.terms_name: <15}",
                f"{self.attachment_flag}",
            ]
        )

    def line_data(self) -> str:
        rows = []
        for line in self.lines:
            rows.extend(
                line.generate_lines(self.internal_number, self.vendor.liableForVat)
            )
        return "\n".join(rows)


@define
class FeederFile:
    invoices: list[Invoice]
    trailer_number: str = "LIB9999999999"

    expense_codes_df: Union[pd.DataFrame, None] = None

    @property
    def batch_total_amount(self) -> float:
        return sum([invoice.amount for invoice in self.invoices])

    @property
    def number_of_invoices(self) -> int:
        return len(self.invoices)

    def _invoice_line_expense_line(self, invoice_line: InvoiceLine) -> None:
        default_condition = (
            self.expense_codes_df["acquisition method"].isnull()  # type: ignore
            & self.expense_codes_df["material type"].isnull()  # type: ignore
            & self.expense_codes_df["order format"].isnull()  # type: ignore
        )
        if invoice_line.poLine is None:
            result = self.expense_codes_df.loc[default_condition]  # type: ignore
        else:
            for row in [
                # Attempts to match on all three conditions
                (
                    (
                        self.expense_codes_df["acquisition method uuid"]  # type: ignore
                        == invoice_line.poLine.acquisitionMethod
                    )
                    & (
                        self.expense_codes_df["material type uuid"]  # type: ignore
                        == invoice_line.poLine.materialType
                    )
                    & (
                        self.expense_codes_df["order format"]  # type: ignore
                        == invoice_line.poLine.orderFormat
                    )
                ),
                # Attempts match when acquisition method is None or doesn't matter
                (
                    (
                        self.expense_codes_df["material type uuid"]  # type: ignore
                        == invoice_line.poLine.acquisitionMethod
                    )
                    & (
                        self.expense_codes_df["order format"]  # type: ignore
                        == invoice_line.poLine.orderFormat
                    )
                ),
                # Attempts match when material type is None
                (
                    (
                        self.expense_codes_df["acquisition method uuid"]  # type: ignore
                        == invoice_line.poLine.acquisitionMethod
                    )
                    & (
                        self.expense_codes_df["order format"]  # type: ignore
                        == invoice_line.poLine.orderFormat
                    )
                ),
                # Attempts match when acquisition method is None
                (
                    (
                        self.expense_codes_df["material type uuid"]  # type: ignore
                        == invoice_line.poLine.materialType
                    )
                    & (
                        self.expense_codes_df["order format"]  # type: ignore
                        == invoice_line.poLine.orderFormat
                    )
                ),
                # Attempts match for Shipping
                (
                    (
                        self.expense_codes_df["acquisition method uuid"]  # type: ignore
                        == invoice_line.poLine.acquisitionMethod
                    )
                    & (
                        self.expense_codes_df["acquisition method"]  # type: ignore
                        == "Shipping"
                    )
                ),
                # Default if all three conditions are None
                default_condition,
            ]:
                result = self.expense_codes_df.loc[row]  # type: ignore
                if len(result) > 0:
                    break
        invoice_line.expense_code = result["Expense code"].values[0]  # type: ignore

    def _populate_expense_code_lookup(self, folio_client):
        acq_methods_lookup, mtypes_lookup = dict(), dict()

        expense_codes_df = pd.DataFrame(expense_codes, dtype=object)

        acquisition_methods = folio_client.get(
            "/orders/acquisition-methods", params={"limit": 250}
        )
        for row in acquisition_methods['acquisitionMethods']:
            acq_methods_lookup[row['value']] = row['id']

        material_types = folio_client.get("/material-types", params={"limit": 250})

        for row in material_types['mtypes']:
            mtypes_lookup[row['name']] = row['id']

        expense_codes_df["acquisition method uuid"] = expense_codes_df[
            "acquisition method"
        ].apply(lambda x: acq_methods_lookup.get(x, np.nan))
        expense_codes_df["material type uuid"] = expense_codes_df[
            "material type"
        ].apply(lambda x: mtypes_lookup.get(x, np.nan))

        self.expense_codes_df = expense_codes_df

    def add_expense_lines(self, folio_client: FolioClient):
        self._populate_expense_code_lookup(folio_client)
        for invoice in self.invoices:
            for line in invoice.lines:
                self._invoice_line_expense_line(line)
        # Sets to None so we don't serialize as JSON
        self.expense_codes_df = None

    def generate(self) -> str:
        raw_file = ""
        for invoice in self.invoices:
            raw_file += f"{invoice.header()}\n"
            raw_file += invoice.line_data()
        raw_file += "\n"
        raw_file += "".join(
            [
                self.trailer_number,
                f"""TR{datetime.utcnow().strftime("%Y%m%d")}""",
                str(self.number_of_invoices),
                f"{self.batch_total_amount:015.2f}",
            ]
        )
        return raw_file
