from datetime import datetime, timedelta
import pytest  # noqa

from unittest.mock import MagicMock

from airflow.providers.sftp.hooks.sftp import SFTPHook

from libsys_airflow.plugins.orafin.payments import (
    generate_file,
    get_invoice,
    init_feeder_file,
    models_converter,
    transfer_to_orafin,
)

from libsys_airflow.plugins.orafin.models import Invoice, FeederFile

invoice_dict = {
    "id": "a6452c96-53ef-4e51-bd7b-aa67ac971133",
    "accountingCode": "804584FEEDER",
    "acqUnitIds": ["bd6c5f05-9ab3-41f7-8361-1c1e847196d3"],
    "invoiceDate": "2023-06-27T00:00:00.000+00:00",
    "fiscalYearId": "e9c45170-2eb3-4207-a1c8-39a51e8b9dd0",
    "folioInvoiceNo": "10596",
    "vendorInvoiceNo": "242428ZP1",
    "subTotal": 135.19,
    "total": 147.53,
    "vendorId": "d7b8ee4b-93c5-4395-90fa-dcc04d26477b",
}

prev_fy_invoice_dict = {
    "id": "previousfy",
    "accountingCode": "804584FEEDER",
    "acqUnitIds": ["bd6c5f05-9ab3-41f7-8361-1c1e847196d3"],
    "invoiceDate": "2023-06-27T00:00:00.000+00:00",
    "fiscalYearId": "200bfabe-07c7-4deb-b54e-99d64a3435cb",
    "folioInvoiceNo": "10596",
    "vendorInvoiceNo": "242428ZP1",
    "subTotal": 135.19,
    "total": 147.53,
    "vendorId": "d7b8ee4b-93c5-4395-90fa-dcc04d26477b",
}

future_invoice_dict = {
    "id": "futureinvoice",
    "accountingCode": "804584FEEDER",
    "acqUnitIds": ["bd6c5f05-9ab3-41f7-8361-1c1e847196d3"],
    "invoiceDate": (datetime.utcnow() + timedelta(days=2)).strftime(
        "%Y-%m-%dT%H:%M:%S.000+00:00"
    ),
    "fiscalYearId": "e9c45170-2eb3-4207-a1c8-39a51e8b9dd0",
    "folioInvoiceNo": "10596",
    "vendorInvoiceNo": "242428ZP1",
    "subTotal": 135.19,
    "total": 147.53,
    "vendorId": "d7b8ee4b-93c5-4395-90fa-dcc04d26477b",
}

no_feeder_invoice_dict = {
    "id": "nofeeder",
    "accountingCode": "804584",
    "acqUnitIds": ["bd6c5f05-9ab3-41f7-8361-1c1e847196d3"],
    "invoiceDate": "2023-06-27T00:00:00.000+00:00",
    "fiscalYearId": "e9c45170-2eb3-4207-a1c8-39a51e8b9dd0",
    "folioInvoiceNo": "10596",
    "vendorInvoiceNo": "242428ZP1",
    "subTotal": 135.19,
    "total": 147.53,
    "vendorId": "d7b8ee4b-93c5-4395-90fa-dcc04d26477b",
}

too_long_invoice_dict = {
    "id": "toolong",
    "accountingCode": "804584FEEDER",
    "acqUnitIds": ["bd6c5f05-9ab3-41f7-8361-1c1e847196d3"],
    "invoiceDate": "2023-06-27T00:00:00.000+00:00",
    "fiscalYearId": "e9c45170-2eb3-4207-a1c8-39a51e8b9dd0",
    "folioInvoiceNo": "10596",
    "vendorInvoiceNo": "242428ZP1abcdefghijklmnopqrstuvwxyz0123456789",
    "subTotal": 135.19,
    "total": 147.53,
    "vendorId": "d7b8ee4b-93c5-4395-90fa-dcc04d26477b",
}

invoice_lines = [
    {
        "adjustmentsTotal": 2.12,
        "id": "484d045a-dcec-40c0-bd1b-2420997df4da",
        "invoiceLineNumber": "1",
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

amount_invoice_lines = [
    {
        "adjustmentsTotal": 6.7,
        "id": "453e5789-afe9-480a-8af2-05c57acd08ed",
        "invoiceLineNumber": "3",
        "subTotal": 50.0,
        "total": 56.7,
        "poLineId": "da1009a8-68ef-4eb3-aaba-8d0e51c6a4ae",
        "fundDistributions": [
            {
                "fundId": "698876aa-180c-4cb8-b865-6e91321122c8",
                "distributionType": "amount",
                "value": 50,
            }
        ],
    }
]

zero_subtotal_invoice_lines = [
    {
        "adjustmentsTotal": 6.7,
        "id": "453e5789-afe9-480a-8af2-05c57acd08ed",
        "invoiceLineNumber": "3",
        "subTotal": 0.0,
        "total": 0.0,
        "poLineId": "da1009a8-68ef-4eb3-aaba-8d0e51c6a4ae",
        "fundDistributions": [
            {
                "fundId": "698876aa-180c-4cb8-b865-6e91321122c8",
                "distributionType": "amount",
                "value": 50,
            }
        ],
    }
]

eresource_po_line = {
    "id": "da1009a8-68ef-4eb3-aaba-8d0e51c6a4ae",
    "acquisitionMethod": "e723e091-1d0a-48f4-9065-61427e723174",
    "eresource": {"materialType": "1a54b431-2e4f-452d-9cae-9cee66c9a892"},
    "orderFormat": "Electronic Resource",
}

acquisition_methods = [
    {"id": "df26d81b-9d63-4ff8-bf41-49bf75cfa70e", "value": "Purchase"},
    {"id": "e723e091-1d0a-48f4-9065-61427e723174", "value": "Subscription"},
]

material_types = [
    {"id": "d9acad2f-2aac-4b48-9097-e6ab85906b25", "name": "text"},
    {"id": "615b8413-82d5-4203-aa6e-e37984cb5ac3", "name": "electronic resource"},
]

ledgers = [
    {"id": "a0d6c701-c316-48d4-bac9-76a34103a3c9"},
    {"id": "a53d9911-7294-4b0c-9a77-88cda7eeb010"},
    {"id": "7fd88aae-6f3b-4cf0-9e98-47989aea23ee"},
]

fiscal_years = [{"id": "e9c45170-2eb3-4207-a1c8-39a51e8b9dd0", "code": "SUL2025"}]

vendor = {
    "code": "HEIN-SUL",
    "erpCode": "012957FEEDER",
    "id": "d7b8ee4b-93c5-4395-90fa-dcc04d26477b",
}


@pytest.fixture
def mock_folio_client():
    def mock_get(*args, **kwargs):
        # Invoice
        if args[0].startswith("/invoice/invoices/a6452c96"):
            return invoice_dict
        elif args[0].startswith("/invoice/invoices/previousfy"):
            return prev_fy_invoice_dict
        elif args[0].startswith("/invoice/invoices/futureinvoice"):
            return future_invoice_dict
        elif args[0].startswith("/invoice/invoices/nofeeder"):
            return no_feeder_invoice_dict
        elif args[0].startswith("/invoice/invoices/toolong"):
            return too_long_invoice_dict
        elif args[0].startswith("/invoice/invoices/"):
            return invoice_dict
        # Invoice Lines
        if args[0].endswith("invoice-lines"):
            if kwargs['query_params']['query'].startswith("invoiceId==e5662732"):
                payload = {"invoiceLines": amount_invoice_lines}
            elif kwargs['query_params']['query'].startswith("invoiceId==zerosubtotal"):
                payload = {"invoiceLines": zero_subtotal_invoice_lines}
            else:
                payload = {"invoiceLines": invoice_lines}
            return payload
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
        if args[0].endswith("8d0e51c6a4ae"):
            return eresource_po_line
        # Organization
        if args[0].endswith("dcc04d26477b"):
            return vendor

        if args[0].endswith("acquisition-methods"):
            return acquisition_methods

        if args[0].endswith("material-types"):
            return material_types

        # Ledger
        if args[0].endswith("ledgers"):
            return {"ledgers": ledgers}

        # Fiscal Year
        if args[0].endswith("current-fiscal-year"):
            return {"code": "SUL2025"}
        if args[0].endswith("fiscal-years"):
            if kwargs['query_params']['query'].startswith("code==SUL2025"):
                return {"fiscalYears": fiscal_years}
        return {}

    mock_client = MagicMock()
    mock_client.folio_get = mock_get
    return mock_client


def test_get_invoice(mock_folio_client):
    converter = models_converter()
    invoice, exclude, exclusion_reason = get_invoice(
        "a6452c96-53ef-4e51-bd7b-aa67ac971133", mock_folio_client, converter
    )

    assert isinstance(invoice, Invoice)
    assert exclude is False
    assert exclusion_reason == ""
    assert invoice.subTotal == 135.19
    assert invoice.acqUnitIds[0] == "bd6c5f05-9ab3-41f7-8361-1c1e847196d3"
    assert len(invoice.lines) == 1
    assert invoice.amount == invoice.subTotal
    assert invoice.internal_number == "LIB10596"
    assert invoice.invoice_type == "DR"
    assert invoice.terms_name == "N30"
    assert invoice.vendor.liableForVat is False
    assert invoice.vendor.vendor_number == "HD012957FEEDER"
    assert invoice.lines[0].tax_exempt is False
    assert invoice.attachment_flag == " "
    assert invoice.lines[0].tax_code(invoice.vendor.liableForVat) == "USE_CA"

    # Tests conditional properties
    invoice.vendor.liableForVat = True
    assert invoice.amount == invoice.total
    invoice.total = -100.00
    assert invoice.invoice_type == "CR"
    invoice.paymentDue = datetime.utcnow()
    assert invoice.terms_name == "IMMEDIATE"
    assert invoice.lines[0].tax_code(True) == "SALES_STANDARD"
    invoice.lines[0].adjustmentsTotal = 0.0
    assert invoice.lines[0].tax_exempt is True
    assert invoice.lines[0].tax_code(True) == "TAX_EXEMPT"
    invoice.paymentTerms = "WILLCALL"
    assert invoice.attachment_flag == "Y"


def test_exclude_invoice(mock_folio_client):
    converter = models_converter()
    invoice, exclude, exclusion_reason = get_invoice(
        "e5662732-489e-489d-96b9-199cabe66a87", mock_folio_client, converter
    )
    assert exclude is True
    assert exclusion_reason == "Amount split"
    assert invoice.lines[0].poLine.orderFormat == "Electronic Resource"


def test_exclude_zero_subtotal(mock_folio_client):
    converter = models_converter()
    invoice, exclude, exclusion_reason = get_invoice(
        "zerosubtotal", mock_folio_client, converter
    )
    assert exclude is True
    assert exclusion_reason == "Zero subtotal"


def test_exclude_prev_fy(mock_folio_client):
    converter = models_converter()
    invoice, exclude, exclusion_reason = get_invoice(
        "previousfy", mock_folio_client, converter
    )
    assert exclude is True
    assert exclusion_reason == "Fiscal year not current"


def test_exclude_future_invoice(mock_folio_client):
    converter = models_converter()
    invoice, exclude, exclusion_reason = get_invoice(
        "futureinvoice", mock_folio_client, converter
    )
    assert exclude is True
    assert exclusion_reason == "Future invoice date"


def test_exclude_nofeeder_invoice(mock_folio_client):
    converter = models_converter()
    invoice, exclude, exclusion_reason = get_invoice(
        "nofeeder", mock_folio_client, converter
    )
    assert exclude is True
    assert exclusion_reason == "Not FEEDER vendor"


def test_exclude_toolong_invoice(mock_folio_client):
    converter = models_converter()
    invoice, exclude, exclusion_reason = get_invoice(
        "toolong", mock_folio_client, converter
    )
    assert exclude is True
    assert exclusion_reason == "Invoice number too long"


def test_generate_file(mock_folio_client):
    feeder_file_dict = {"invoices": [invoice_dict]}

    feeder_file = generate_file(feeder_file_dict, mock_folio_client)

    assert isinstance(feeder_file, FeederFile)


def test_init_feeder_file(mock_folio_client):
    converter = models_converter()
    invoice_dict["vendor"] = vendor
    invoice_dict["lines"] = invoice_lines

    invoices = [invoice_dict]

    feeder_file = init_feeder_file(invoices, mock_folio_client, converter)

    assert isinstance(feeder_file, FeederFile)
    assert feeder_file.batch_total_amount == 135.19
    assert feeder_file.number_of_invoices == 1

    feeder_file.add_expense_lines(mock_folio_client)
    assert feeder_file.invoices[0].lines[0].expense_code == '53245'


@pytest.fixture
def mock_sftp_hook(mocker):
    def mockstore_file(*args):
        pass

    mock_hook = mocker.patch("libsys_airflow.plugins.orafin.payments.SFTPHook")
    mock_hook.__class__ = SFTPHook
    mock_hook.store_file = mockstore_file
    return mock_hook


def test_transfer_to_orafin(mock_sftp_hook, tmp_path, caplog):
    feeder_file_path = tmp_path / "feeder20210823_202309240000"
    feeder_file_path.write_text(
        """LIB376992    HD006169FEEDER         23-13094 376992                         20230503000000000385.52DR                              N30
LIB376992    DR000000000023.95USE_CA              1065087-101-AALIB-53245
LIB376992    TX000000000002.19USE_CA              1065087-101-AALIB-53245"""
    )
    transfer_to_orafin(str(feeder_file_path))

    assert feeder_file_path.exists()
    assert """Command ['scp', '-i /opt/airflow/vendor-keys/""" in caplog.text
