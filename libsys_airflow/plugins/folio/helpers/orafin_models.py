from datetime import datetime

import numpy as np
import pandas as pd

from attrs import define
from typing import Union

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
    paymentDate: Union[datetime, None] = None
    paymentTerms: Union[str, None] = None

    @property
    def amount(self):
        if self.vendor and self.vendor.liableForVat:
            return self.total
        return self.subTotal

    @property
    def attachment_flag(self):
        if self.paymentTerms and self.paymentTerms.startswith("WILLCALL"):
            return "Y"

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
        if self.paymentDate:
            return "IMMEDIATE"
        return "N30"


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
