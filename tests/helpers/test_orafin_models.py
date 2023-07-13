import datetime
import uuid

import pytest  # noqa

from unittest.mock import MagicMock

from libsys_airflow.plugins.folio.helpers.orafin_models import (
    FeederFile,
    Fund,
    fundDistribution,
    Invoice,
    InvoiceLine,
    PurchaseOrderLine,
    Vendor,
)

acquisition_methods = [
    {
        'id': '041035ad-b2a4-4aa0-b6a5-234b88bf938c',
        'value': 'Demand Driven Acquisitions (DDA)',
    },
    {'id': '0a4163a5-d225-4007-ad90-2fb41b73efab', 'value': 'Gift'},
    {'id': 'da6703b1-81fe-44af-927a-94f24d1ab8ee', 'value': 'Other'},
    {
        'id': '306489dd-0053-49ee-a068-c316444a8f55',
        'value': 'Purchase At Vendor System',
    },
    {'id': 'df26d81b-9d63-4ff8-bf41-49bf75cfa70e', 'value': 'Purchase'},
    {'id': '0c9b09c9-b94f-4702-aa63-a7f43617a225', 'value': 'Internal transfer'},
    {'id': 'd2420b93-7b93-41b7-8b42-798f64cb6dd2', 'value': 'Depository'},
    {'id': '86d12634-b848-4968-adf0-5a95ce41c41b', 'value': 'Free'},
    {'id': '5771a8a4-9323-49ee-9002-1b068d71ff42', 'value': 'Membership'},
    {'id': 'd0d3811c-19f8-4c57-a462-958165cdbbea', 'value': 'Technical'},
    {'id': '8a33895e-2c69-4a98-ab48-b7ec1fa852d0', 'value': 'Exchange'},
    {'id': '796596c4-62b5-4b64-a2ce-524c747afaa2', 'value': 'Approval Plan'},
    {
        'id': 'aaa541f3-39d2-4887-ab8f-6ba12d08ca52',
        'value': 'Evidence Based Acquisitions (EBA)',
    },
    {'id': '2cd47690-7f73-4a32-93ce-3b6e7fec07af', 'value': 'Blanket'},
    {'id': '8b30683a-2dd9-4d56-be78-6c8e971617a5', 'value': 'Exchange sent'},
    {'id': '28b0e1ee-ed97-4154-b78e-8f9b427726e7', 'value': 'Exchange received'},
    {'id': '8cbc15cc-23d2-4b9c-a163-33932101905a', 'value': 'Maintenance fee'},
    {'id': 'e2ce5ac8-2d48-45e8-b39c-c2d02e17a366', 'value': 'Shared purchase'},
    {'id': '0ccaeb03-453a-4798-93e6-714a52acb244', 'value': 'Expense transfer'},
    {'id': '79f06560-e77f-4513-93a8-a66b6dce113c', 'value': 'Standing order'},
    {'id': 'e723e091-1d0a-48f4-9065-61427e723174', 'value': 'Subscription'},
    {'id': 'f1e7e2ad-be2c-43b0-9cb3-35c3cffd52f6', 'value': 'Shipping'},
    {'id': 'c9757d83-e67c-45a4-a03a-6b0c60b01b53', 'value': 'Prepaid'},
    {'id': 'a8737ea5-c500-41a0-8d17-0390ada22727', 'value': 'Package'},
]

mtypes = [
    {'id': 'd9acad2f-2aac-4b48-9097-e6ab85906b25', 'name': 'text'},
    {'id': 'fd6c6515-d470-4561-9c32-3e3290d4ca98', 'name': 'microform'},
    {'id': '615b8413-82d5-4203-aa6e-e37984cb5ac3', 'name': 'electronic resource'},
    {'id': '5ee11d91-f7e8-481d-b079-65d708582ccc', 'name': 'dvd'},
    {'id': '1a54b431-2e4f-452d-9cae-9cee66c9a892', 'name': 'book'},
    {'id': '30b3e36a-d3b2-415e-98c2-47fbdf878862', 'name': 'video recording'},
    {'id': '71fbd940-1027-40a6-8a48-49b44d795e46', 'name': 'unspecified'},
    {'id': 'dd0bf600-dbd9-44ab-9ff2-e2a61a6539f1', 'name': 'sound recording'},
    {'id': '81330e72-a104-4c09-be94-77c4c0f10f51', 'name': 'accessories 1'},
    {'id': '185aae82-8ac0-4f64-a022-6f5476b0eaa5', 'name': 'accessories 2'},
    {'id': '16e4a925-4e84-4199-84e0-2cdd95973080', 'name': 'accessories 3'},
    {'id': 'f66297cb-a876-437c-b98f-5b0f604d0c45', 'name': 'accessories 4'},
    {'id': '69edaa1b-e40b-4f1c-8cb5-4b615ac6a664', 'name': 'archival'},
    {'id': '58973afa-1b0a-4ff7-b463-c2e946c8fb00', 'name': 'av equipment 1'},
    {'id': 'a3cc5a80-1bf5-42a5-b333-b9d201b650a0', 'name': 'av equipment 2'},
    {'id': '86cf4f25-e8c9-4486-8583-e1d75bf5a63b', 'name': 'av equipment 3'},
    {'id': '4a61a6f2-11d7-4d47-a39f-0b2712522a23', 'name': 'database'},
    {'id': '23726e5a-712e-46a2-b0eb-37b955f42914', 'name': 'dataset'},
    {'id': '80e9f76c-766f-46c5-988a-b8fac5204604', 'name': 'kit'},
    {'id': 'a71b6ca2-9f2d-4ab9-bf5d-1ad8475607d8', 'name': 'laptop'},
    {'id': '7f9c4fab-138c-48dd-bc4a-d0db03279b3e', 'name': 'library equipment 1'},
    {'id': '8533857f-662f-456b-ad41-c57f2cbb67b0', 'name': 'library equipment 2'},
    {'id': '2095d272-341f-4c5a-896e-313caff66995', 'name': 'library equipment 3'},
    {'id': '794de86f-ecbc-45ad-b790-f30eb19797ec', 'name': 'multimedia'},
    {'id': '1c092366-5f0b-42c7-b7cb-989e1dc7a378', 'name': 'portable device 1'},
    {'id': '9b0eb098-a209-4445-b410-0a717c6e4643', 'name': 'software'},
    {'id': '60c6bf6d-2a29-4fbc-9461-056699e740e7', 'name': 'map'},
    {'id': 'd934e614-215d-4667-b231-aed97887f289', 'name': 'periodical'},
    {'id': 'e51f66f2-e5f6-41c3-bef5-26557bae7c12', 'name': 'portable device 2'},
    {'id': 'a2253fdc-5808-4f2b-9eb3-e43b3884ab33', 'name': 'portable device 3'},
    {'id': '8cea2cd7-6a61-494e-a602-17045da7e3cb', 'name': 'score'},
]


@pytest.fixture
def mock_folio_client():
    def mock_get(*args, **kwargs):
        output = {}
        if args[0].endswith("acquisition-methods"):
            output = {"acquisitionMethods": acquisition_methods}
        if args[0].endswith("material-types"):
            output = {"mtypes": mtypes}
        return output

    mock_client = MagicMock()
    mock_client.get = mock_get
    return mock_client


@pytest.fixture
def mock_invoice():
    return Invoice(
        id="fd6e5f34-101e-4dd2-8542-0fdaf7713a2b",
        accountingCode="668330FEEDER",
        folioInvoiceNo="10592",
        invoiceDate=datetime.datetime(2023, 7, 12),
        lines=[
            InvoiceLine(
                id="b26f45bd-aa92-471d-9aa9-a27d5f520a78",
                adjustmentsTotal=34.24,
                fundDistributions=[
                    fundDistribution(
                        distributionType="percentage",
                        value=100.0,
                        fund=Fund(
                            id="96750a26-90a9-47cd-94a2-1b910e824d7e",
                            externalAccountNo="1065031-111-KBAEW",
                        ),
                    )
                ],
                poLine=PurchaseOrderLine(
                    id=str(uuid.uuid4()),
                    acquisitionMethod="e723e091-1d0a-48f4-9065-61427e723174",
                    materialType="dd0bf600-dbd9-44ab-9ff2-e2a61a6539f1",
                    orderFormat="Physical Resource",
                ),
                subTotal=375.03,
                total=409.24,
            )
        ],
        subTotal=1442.03,
        total=1572.1,
        vendor=Vendor(
            code="ANTIPODEAN-SUL",
            erpCode="668330FEEDER",
            id="3b114160-5312-4268-8bf9-4da5b193bb1a",
            liableForVat=False,
        ),
        vendorInvoiceNo="15142",
    )


def test_expense_codes(mock_folio_client):
    vendor = Vendor(id="abcdef", code="97236", erpCode="ef1244", liableForVat=False)

    # Range of different Purchace Order Lines
    db_electronic_purchase = PurchaseOrderLine(
        id=str(uuid.uuid4()),
        acquisitionMethod="df26d81b-9d63-4ff8-bf41-49bf75cfa70e",
        materialType="4a61a6f2-11d7-4d47-a39f-0b2712522a23",
        orderFormat="Electronic Resource",
    )

    db_electronic_subscription = PurchaseOrderLine(
        id=str(uuid.uuid4()),
        acquisitionMethod="e723e091-1d0a-48f4-9065-61427e723174",
        materialType="4a61a6f2-11d7-4d47-a39f-0b2712522a23",
        orderFormat="Electronic Resource",
    )

    book_electronic_approval_plan = PurchaseOrderLine(
        id=str(uuid.uuid4()),
        acquisitionMethod="796596c4-62b5-4b64-a2ce-524c747afaa2",
        materialType="1a54b431-2e4f-452d-9cae-9cee66c9a892",
        orderFormat="Electronic Resource",
    )

    book_electronic_subscription = PurchaseOrderLine(
        id=str(uuid.uuid4()),
        acquisitionMethod="e723e091-1d0a-48f4-9065-61427e723174",
        materialType="1a54b431-2e4f-452d-9cae-9cee66c9a892",
        orderFormat="Electronic Resource",
    )

    periodical_electronic_purchase = PurchaseOrderLine(
        id=str(uuid.uuid4()),
        acquisitionMethod="df26d81b-9d63-4ff8-bf41-49bf75cfa70e",
        materialType="d934e614-215d-4667-b231-aed97887f289",
        orderFormat="Electronic Resource",
    )

    maintenance_fee_electronic = PurchaseOrderLine(
        id=str(uuid.uuid4()),
        acquisitionMethod="8cbc15cc-23d2-4b9c-a163-33932101905a",
        materialType=None,
        orderFormat="Electronic Resource",
    )

    sound_recording_physical_purchase = PurchaseOrderLine(
        id=str(uuid.uuid4()),
        acquisitionMethod="e723e091-1d0a-48f4-9065-61427e723174",
        materialType="dd0bf600-dbd9-44ab-9ff2-e2a61a6539f1",
        orderFormat="Physical Resource",
    )

    software_physical_subscription = PurchaseOrderLine(
        id=str(uuid.uuid4()),
        acquisitionMethod="df26d81b-9d63-4ff8-bf41-49bf75cfa70e",
        materialType="9b0eb098-a209-4445-b410-0a717c6e4643",
        orderFormat="Physical Resource",
    )

    shipping = PurchaseOrderLine(
        id=str(uuid.uuid4()),
        acquisitionMethod="f1e7e2ad-be2c-43b0-9cb3-35c3cffd52f6",
        materialType=None,
        orderFormat=None,
    )

    match_default = PurchaseOrderLine(
        id=str(uuid.uuid4()),
        acquisitionMethod="e2ce5ac8-2d48-45e8-b39c-c2d02e17a366",
        materialType="a71b6ca2-9f2d-4ab9-bf5d-1ad8475607d8",
        orderFormat="Physical Resource",
    )

    invoice = Invoice(
        id="abcdefa",
        invoiceDate=datetime.datetime(2023, 6, 28),
        folioInvoiceNo="12356",
        accountingCode="4567",
        subTotal=100.00,
        total=110.00,
        vendorInvoiceNo="abc12345",
        vendor=vendor,
        lines=[
            InvoiceLine(
                id=str(uuid.uuid4()),
                adjustmentsTotal=0.00,
                subTotal=0.00,
                total=0.00,
                poLine=db_electronic_purchase,
            ),
            InvoiceLine(
                id=str(uuid.uuid4()),
                adjustmentsTotal=0.00,
                subTotal=0.00,
                total=0.00,
                poLine=db_electronic_subscription,
            ),
            InvoiceLine(
                id=str(uuid.uuid4()),
                adjustmentsTotal=0.00,
                subTotal=0.00,
                total=0.00,
                poLine=book_electronic_approval_plan,
            ),
            InvoiceLine(
                id=str(uuid.uuid4()),
                adjustmentsTotal=0.00,
                subTotal=0.00,
                total=0.00,
                poLine=book_electronic_subscription,
            ),
            InvoiceLine(
                id=str(uuid.uuid4()),
                adjustmentsTotal=0.00,
                subTotal=0.00,
                total=0.00,
                poLine=periodical_electronic_purchase,
            ),
            InvoiceLine(
                id=str(uuid.uuid4()),
                adjustmentsTotal=0.00,
                subTotal=0.00,
                total=0.00,
                poLine=maintenance_fee_electronic,
            ),
            InvoiceLine(
                id=str(uuid.uuid4()),
                adjustmentsTotal=0.00,
                subTotal=0.00,
                total=0.00,
                poLine=sound_recording_physical_purchase,
            ),
            InvoiceLine(
                id=str(uuid.uuid4()),
                adjustmentsTotal=0.00,
                subTotal=0.00,
                total=0.00,
                poLine=software_physical_subscription,
            ),
            InvoiceLine(
                id=str(uuid.uuid4()),
                adjustmentsTotal=0.00,
                subTotal=0.00,
                total=0.00,
                poLine=shipping,
            ),
            InvoiceLine(
                id=str(uuid.uuid4()),
                adjustmentsTotal=0.00,
                subTotal=0.00,
                total=0.00,
                poLine=match_default,
            ),
        ],
    )

    feeder_file = FeederFile(invoices=[invoice])
    feeder_file.add_expense_lines(mock_folio_client)

    assert feeder_file.invoices[0].lines[0].expense_code == "53258"
    assert feeder_file.invoices[0].lines[1].expense_code == "53263"
    assert feeder_file.invoices[0].lines[2].expense_code == "53256"
    assert feeder_file.invoices[0].lines[3].expense_code == "53261"
    assert feeder_file.invoices[0].lines[4].expense_code == "53257"
    assert feeder_file.invoices[0].lines[5].expense_code == "53270"
    assert feeder_file.invoices[0].lines[6].expense_code == "53270"
    assert feeder_file.invoices[0].lines[7].expense_code == "55410"
    assert feeder_file.invoices[0].lines[8].expense_code == "55320"
    assert feeder_file.invoices[0].lines[9].expense_code == "53245"

    feeder_file.invoices[0].lines[0].poLine = None
    feeder_file.add_expense_lines(mock_folio_client)
    assert feeder_file.invoices[0].lines[0].expense_code == "53245"


def test_invoice_header(mock_invoice):
    raw_header = mock_invoice.header()

    assert len(raw_header) == 147

    # Internal Number
    assert raw_header[0:13] == "LIB10592     "

    # Vendor Number and Vendor Site Code
    assert raw_header[13:36] == "HD668330FEEDER         "

    # Invoice Number
    assert raw_header[36:76] == "15142 10592                             "

    # Invoice Date
    assert raw_header[76:84] == "20230712"

    # Invoice Amount
    assert raw_header[84:99] == "000000001442.03"

    # Invoice Type
    assert raw_header[99:131] == "DR                              "

    # Terms name
    assert raw_header[131:145] == "N30           "

    # Attachment flag
    assert raw_header[146] == " "


def test_invoice_amount(mock_invoice):
    assert mock_invoice.amount == 1442.03
    mock_invoice.vendor.liableForVat = True
    assert mock_invoice.amount == 1572.1


def test_attachment_flag(mock_invoice):
    assert mock_invoice.attachment_flag == " "
    mock_invoice.paymentTerms = "WILLCALL"
    assert mock_invoice.attachment_flag == "Y"


def test_terms_name(mock_invoice):
    assert mock_invoice.terms_name == "N30"
    mock_invoice.paymentDue = datetime.datetime(2023, 7, 13)
    assert mock_invoice.terms_name == "IMMEDIATE"


def test_invoice_type(mock_invoice):
    assert mock_invoice.invoice_type == "DR"
    mock_invoice.subTotal = -1.0
    assert mock_invoice.invoice_type == "CR"


def test_invoice_lines_generate_lines(mock_invoice):
    for line in mock_invoice.lines:
        line.expense_code = '53245'

    raw_lines = mock_invoice.line_data()
    dr_line, tx_line, ta_line = raw_lines.splitlines()

    assert len(dr_line) == 119
    assert len(tx_line) == 119
    assert len(ta_line) == 119

    # DR Internal Number
    assert dr_line[0:13] == "LIB10592     "

    assert dr_line[13:15] == "DR"

    assert dr_line[15:30] == "000000000375.03"

    assert dr_line[30:50] == "USE_CA              "

    assert dr_line[50:72] == "1065031-111-KBAEW-5324"

    # TX Internal Number
    assert tx_line[0:13] == "LIB10592     "

    assert tx_line[13:15] == "TX"

    assert tx_line[15:30] == "000000000034.24"

    assert tx_line[30:50] == "USE_CA              "

    assert tx_line[50:72] == "1065031-111-KBAEW-5324"

    # TA Internal Number
    assert ta_line[0:13] == "LIB10592     "

    assert ta_line[13:15] == "TA"

    assert ta_line[15:30] == "-00000000034.24"

    assert ta_line[30:50] == "USE_CA              "


def test_split_percentage_invoice_lines():
    invoice_line = InvoiceLine(
        adjustmentsTotal=41.08,
        id='eb96354a-c3d7-4faf-b6aa-21e10c0dc5c4',
        subTotal=450.0,
        total=491.08,
        expense_code='53245',
        poLine=PurchaseOrderLine(
            id='3b793afc-904b-4daf-94e9-9b7ac0445113',
            acquisitionMethod='df26d81b-9d63-4ff8-bf41-49bf75cfa70e',
            orderFormat='Physical Resource',
            materialType='1a54b431-2e4f-452d-9cae-9cee66c9a892',
        ),
        fundDistributions=[
            fundDistribution(
                distributionType='percentage',
                value=75.0,
                fund=Fund(
                    id='15c375e7-ee16-4b3d-8370-1a02bb3d909d',
                    externalAccountNo='1065032-101-KARFD',
                ),
            ),
            fundDistribution(
                distributionType='percentage',
                value=25.0,
                fund=Fund(
                    id='12347aee-492b-4d73-b7fd-b5d113227f89',
                    externalAccountNo='1065090-101-KATMX',
                ),
            ),
        ],
    )
    lines = invoice_line.generate_lines("LIB10592", False)

    assert len(lines) == 6

    assert float(lines[0][15:30]) == 337.5
    assert float(lines[1][15:30]) == 30.81
    assert float(lines[2][15:30]) == -30.81

    assert float(lines[3][15:30]) == 112.5
    assert float(lines[4][15:30]) == 10.27

    assert (
        float(lines[1][15:30]) + float(lines[4][15:30]) == invoice_line.adjustmentsTotal
    )


def test_invoice_line_fund_dist_amt():
    invoice_line = InvoiceLine(
        id=str(uuid.uuid4()),
        adjustmentsTotal=0.00,
        subTotal=40.00,
        total=0.00,
        expense_code="53245",
        fundDistributions=[
            fundDistribution(
                distributionType="amount",
                value=40.00,
                fund=Fund(
                    id="12347aee-492b-4d73-b7fd-b5d113227f89",
                    externalAccountNo='1065090-101-KATMX',
                ),
            )
        ],
    )

    lines = invoice_line.generate_lines("LIB10592", True)

    assert lines[0][15:30] == "000000000040.00"


def test_invoice_line_tax_code():
    invoice_line = InvoiceLine(
        id=str(uuid.uuid4()),
        adjustmentsTotal=0.00,
        subTotal=0.00,
        total=0.00,
    )

    assert invoice_line.tax_code(False) == "TAX_EXEMPT"

    invoice_line.adjustmentsTotal = 1.00

    assert invoice_line.tax_code(True) == "SALES_STANDARD"


def test_vendor_number(mock_invoice):
    assert mock_invoice.vendor.vendor_number == "HD668330FEEDER"


def test_feeder_file(mock_invoice, mock_folio_client):
    feeder_file = FeederFile(invoices=[mock_invoice])

    assert feeder_file.batch_total_amount == 1442.03
    assert feeder_file.number_of_invoices == 1

    feeder_file.add_expense_lines(mock_folio_client)

    raw_feeder_file = feeder_file.generate()

    assert raw_feeder_file.splitlines()[-1] == "LIB9999999999TR202307131000000001442.03"
