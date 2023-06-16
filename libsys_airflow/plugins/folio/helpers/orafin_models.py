import jmespath

from datetime import datetime

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
    orderFormat: str
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

    @property
    def amount(self):
        if self.vendor and self.vendor.liableForVat:
            return self.total
        return self.subTotal

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

    expense_codes: dict = expense_codes

    @property
    def batch_total_amount(self) -> float:
        return sum([invoice.amount for invoice in self.invoices])

    @property
    def number_of_invoices(self) -> int:
        return len(self.invoices)

    def _invoice_line_expense_line(self, invoice_line: InvoiceLine):
        default_expense_code = '53245'
        if invoice_line.poLine is None:
            invoice_line.expense_code = default_expense_code
            return
        query_template = "[*].{{key: @, order_format: order_format, acquisition_method_uuid: acquisition_method_uuid, material_type_uuid: material_type_uuid}} | [?order_format == '{order_format_value}' && contains(acquisition_method_uuid, '{acquisition_method_uuid_value}') && contains(material_type_uuid, '{material_type_uuid_value}')].key"
        expense_code = jmespath.search(
            query_template.format(
                order_format_value=invoice_line.poLine.orderFormat,
                acquisition_method_uuid_value=invoice_line.poLine.acquisitionMethod,
                material_type_uuid_value=invoice_line.poLine.materialType,
            ),
            self.expense_codes,
        )
        if expense_code is None:
            invoice_line.expense_code = default_expense_code
        else:
            invoice_line.expense_code = expense_code[0]

    def _populate_expense_code_lookup(self, folio_client):
        acquisition_methods = folio_client.get(
            "/orders/acquisition-methods", params={"limit": 250}
        )

        material_types = folio_client.get("/material-types", params={"limit": 250})

        acq_method_query = "acquisitionMethods[?value =='{0}'].id"
        material_type_query = "mtypes[?name =='{0}'].id"

        for row in self.expense_codes.values():
            if len(row['acquisition_method']) > 0:
                row['acquisition_method_uuid'] = jmespath.search(
                    acq_method_query.format(row['acquisition_method']),
                    acquisition_methods,
                )
            if len(row.get('material_type', [])) > 0:
                row['material_type_uuid'] = jmespath.search(
                    material_type_query.format(row['material_type']), material_types
                )

    def add_expense_lines(self, folio_client: FolioClient):
        self._populate_expense_code_lookup(folio_client)
        for invoice in self.invoices:
            for line in invoice.lines:
                self._invoice_line_expense_line(line)
