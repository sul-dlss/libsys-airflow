from datetime import datetime
import pytest  # noqa

from unittest.mock import MagicMock

from libsys_airflow.plugins.folio.orafin_payments import (
    _get_invoice,
    _init_feeder_file,
    _models_converter,
)

from libsys_airflow.plugins.folio.helpers.orafin_models import Invoice, FeederFile

invoice = {
    "id": "a6452c96-53ef-4e51-bd7b-aa67ac971133",
    "accountingCode": "804584FEEDER",
    "invoiceDate": "2023-06-27T00:00:00.000+00:00",
    "folioInvoiceNo": "10596",
    "vendorInvoiceNo": "242428ZP1",
    "subTotal": 135.19,
    "total": 147.53,
    "vendorId": "d7b8ee4b-93c5-4395-90fa-dcc04d26477b",
}

invoice_lines = [
    {
        "adjustmentsTotal": 2.12,
        "id": "484d045a-dcec-40c0-bd1b-2420997df4da",
        "subTotal": 23.19,
        "total": 25.31,
        "poLineId": "b793afc-904b-4daf-94e9-9b7ac0445113",
        "fundDistributions": [
            {
                "fundId": "698876aa-180c-4cb8-b865-6e91321122c8",
                "distributionType": "percentage",
                "value": 100.0,
            }
        ],
    }
]

po_line = {
    "id": "b793afc-904b-4daf-94e9-9b7ac0445113",
    "acquisitionMethod": "df26d81b-9d63-4ff8-bf41-49bf75cfa70e",
    "orderFormat": "Physical Resource",
    "physical": {"materialType": "1a54b431-2e4f-452d-9cae-9cee66c9a892"},
}

acquisition_methods = [
    {"id": "df26d81b-9d63-4ff8-bf41-49bf75cfa70e", "value": "Purchase"},
    {"id": "e723e091-1d0a-48f4-9065-61427e723174", "value": "Subscription"},
]

material_types = [
    {"id": "d9acad2f-2aac-4b48-9097-e6ab85906b25", "name": "text"},
    {"id": "615b8413-82d5-4203-aa6e-e37984cb5ac3", "name": "electronic resource"},
]

vendor = {
    "code": "HEIN-SUL",
    "erpCode": "012957FEEDER",
    "id": "d7b8ee4b-93c5-4395-90fa-dcc04d26477b",
    "liableForVat": False,
}


@pytest.fixture
def mock_folio_client():
    def mock_get(*args, **kwargs):
        # Invoice
        if args[0].endswith("aa67ac971133"):
            return invoice
        # Invoice Lines
        if args[0].endswith("invoice-lines"):
            return {"invoiceLines": invoice_lines}
        # Fund
        if args[0].endswith("6e91321122c8"):
            return {
                "fund": {
                    "id": "698876aa-180c-4cb8-b865-6e91321122c8",
                    "externalAccountNo": "1065084-101-AALIB",
                }
            }
        # PO Line
        if args[0].endswith("9b7ac0445113"):
            return po_line
        # Organization
        if args[0].endswith("dcc04d26477b"):
            return vendor

        if args[0].endswith("acquisition-methods"):
            return {"acquisitionMethods": acquisition_methods}

        if args[0].endswith("material-types"):
            return {"mtypes": material_types}
        return {}

    mock_client = MagicMock()
    mock_client.get = mock_get
    return mock_client


def test_get_invoice(mock_folio_client):
    converter = _models_converter()
    invoice = _get_invoice(
        "a6452c96-53ef-4e51-bd7b-aa67ac971133", mock_folio_client, converter
    )

    assert isinstance(invoice, Invoice)
    assert invoice.subTotal == 135.19
    assert len(invoice.lines) == 1
    assert invoice.amount == invoice.subTotal
    assert invoice.internal_number == "LIB10596"
    assert invoice.invoice_type == "DR"
    assert invoice.terms_name == "N30"
    assert invoice.vendor.liableForVat is False
    assert invoice.vendor.vendor_number == "HD012957FEEDER"
    assert invoice.lines[0].tax_exempt is False
    assert invoice.attachment_flag is None
    assert invoice.lines[0].tax_code(invoice.vendor.liableForVat) == "USE_CA"

    # Tests conditional properties
    invoice.vendor.liableForVat = True
    assert invoice.amount == invoice.total
    invoice.total = -100.00
    assert invoice.invoice_type == "CR"
    invoice.paymentDate = datetime.utcnow()
    assert invoice.terms_name == "IMMEDIATE"
    assert invoice.lines[0].tax_code(True) == "SALES_STANDARD"
    invoice.lines[0].adjustmentsTotal = 0.0
    assert invoice.lines[0].tax_exempt is True
    assert invoice.lines[0].tax_code(True) == "TAX_EXEMPT"
    invoice.paymentTerms = "WILLCALL"
    assert invoice.attachment_flag == "Y"


def test_init_feeder_file(mock_folio_client):
    invoices = [invoice]
    converter = _models_converter()
    feeder_file = _init_feeder_file(invoices, mock_folio_client, converter)

    assert isinstance(feeder_file, FeederFile)
    assert feeder_file.batch_total_amount == 135.19
    assert feeder_file.number_of_invoices == 1

    feeder_file.add_expense_lines(mock_folio_client)
    assert feeder_file.invoices[0].lines[0].expense_code == '53245'
