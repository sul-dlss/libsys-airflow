import pytest  # noqa

from unittest.mock import MagicMock

from libsys_airflow.plugins.folio.invoices import (
    invoices_awaiting_payment_task,
    invoices_paid_within_date_range,
    invoice_lines_from_invoices,
    _get_ids_from_vouchers,
    _get_all_ids_from_invoices,
)

vouchers = [{"invoiceId": 'a6452c96-53ef-4e51-bd7b-aa67ac971133'}]
invoices = [
    {"id": "649c0a8e-6741-49a1-a8a9-de1b8c01358f"},
    {"id": "4d9f89f6-c2b0-49f8-bdff-fc425b980057"},
]
invoices_date_range = [{"id": "34cabbbd-d419-4853-ad3a-d0eafd4310c6"}]


@pytest.fixture
def mock_invoice_lines():
    return [
        {
            "id": "cb0baa2d-7dd7-4986-8dc7-4909bbc18ce6",
            "fundDistributions": [
                {
                    "code": "ABBOTT-SUL",
                    "encumbrance": "cfb59e90-014a-4860-9f5e-bdcfbf1a9f6f",
                    "fundId": "3eb86c5f-c77b-4cc9-8f29-7de7ce313411",
                    "distributionType": "percentage",
                    "value": 100.0,
                }
            ],
            "invoiceId": "29f339e3-dfdc-43e4-9442-eb817fdfb069",
            "invoiceLineNumber": "10",
            "invoiceLineStatus": "Paid",
            "poLineId": "be0af62c-665e-4178-ae13-e3250d89bcc6",
        },
        {
            "id": "318e17ff-f36a-407d-aa9b-68bde2cf3dd0",
            "fundDistributions": [
                {
                    "code": "JACKMAND-SUL",
                    "encumbrance": "ae319e1f-3fb8-449e-9e83-7ffeb4bee3b9",
                    "fundId": "3f59fef1-c089-4ce5-a8c0-00e480346f67",
                    "distributionType": "percentage",
                    "value": 100.0,
                }
            ],
            "invoiceId": "29f339e3-dfdc-43e4-9442-eb817fdfb069",
            "invoiceLineNumber": "11",
            "invoiceLineStatus": "Paid",
            "poLineId": "496b5a5d-40c0-49cd-a874-13d5e6ce23cf",
        },
        {
            "id": "5c6cffcf-1951-47c9-817f-145cbe931dea",
            "invoiceId": "2dcebfd3-82b0-429d-afbb-dff743602bea",
            "invoiceLineNumber": "29",
            "invoiceLineStatus": "Paid",
            "poLineId": "d55342ce-0a33-4aa2-87c6-5ad6e1a12b75",
        },
    ]


@pytest.fixture
def mock_folio_client(mock_invoice_lines):
    def mock_get(*args, **kwargs):
        # Vouchers
        if args[0].startswith("/voucher/vouchers"):
            return vouchers

    def mock_get_all(*args, **kwargs):
        # Invoice
        if args[0].startswith("/invoice-storage/invoices"):
            if kwargs["query"].startswith(
                '?query=((paymentDate>="2023-08-28 00:00:33.619135+00:00") and status=="Paid")'
            ):
                return invoices
            else:
                return invoices_date_range
        # Invoice Lines
        if args[0].endswith("invoice-lines"):
            if kwargs["query"].startswith("?query=(invoiceId==29f339e3"):
                return [mock_invoice_lines[0], mock_invoice_lines[1]]
            elif kwargs["query"].startswith("?query=(invoiceId==2dcebfd3"):
                return [mock_invoice_lines[2]]
            else:
                return mock_invoice_lines

    mock_client = MagicMock()
    mock_client.folio_get = mock_get
    mock_client.folio_get_all = mock_get_all
    return mock_client


@pytest.fixture
def mock_folio_client_no_paid_invoices():
    def mock_get(*args, **kwargs):
        return []

    def mock_get_all(*args, **kwargs):
        return []

    mock_client = MagicMock()
    mock_client.folio_get = mock_get
    mock_client.folio_get_all = mock_get_all
    return mock_client


@pytest.fixture
def mock_scheduled_dag_run(mocker):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "scheduled__2024-09-19"
    dag_run.data_interval_end = "2024-09-18T09:00:00+00:00"
    dag_run.data_interval_start = "2024-09-11T09:00:00+00:00"

    return dag_run


@pytest.fixture
def mock_manual_dag_run(mocker):
    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual__2024-09-19"
    dag_run.data_interval_end = ("2023-08-23T09:00:00+00:00",)
    dag_run.data_interval_start = ("2023-08-16T09:00:00+00:00",)

    return dag_run


def test_get_vouchers(mock_folio_client):
    invoice_ids = _get_ids_from_vouchers("exportToAccounting=true", mock_folio_client)
    assert invoice_ids[0] == 'a6452c96-53ef-4e51-bd7b-aa67ac971133'


def test_invoices_awaiting_payment_task(mocker, mock_folio_client):
    mocker.patch(
        "libsys_airflow.plugins.folio.invoices._folio_client",
        return_value=mock_folio_client,
    )

    invoice_ids = invoices_awaiting_payment_task.function()
    assert invoice_ids[0] == 'a6452c96-53ef-4e51-bd7b-aa67ac971133'


def test_get_invoices(mock_folio_client, mock_manual_dag_run):
    date = "2023-08-28 00:00:33.619135+00:00"
    invoice_ids = _get_all_ids_from_invoices(
        f"""?query=((paymentDate>="{date}") and status=="Paid")""",
        mock_folio_client,
    )
    assert invoice_ids[0] == "649c0a8e-6741-49a1-a8a9-de1b8c01358f"


def test_invoices_paid_since_beginning(
    mocker, mock_folio_client, mock_manual_dag_run, caplog
):
    mocker.patch(
        "libsys_airflow.plugins.folio.invoices._folio_client",
        return_value=mock_folio_client,
    )
    params = {"logical_date": "2023-08-28 00:00:33.619135+00:00"}
    invoice_ids = invoices_paid_within_date_range.function(
        dag_run=mock_manual_dag_run, params=params
    )
    from_date = params.get("logical_date")
    assert invoice_ids[0] == "649c0a8e-6741-49a1-a8a9-de1b8c01358f"
    assert f"Querying paid invoices with paymentDate >= {from_date}" in caplog.text


def test_invoices_paid_within_date_range(
    mocker, mock_folio_client, mock_scheduled_dag_run, caplog
):
    mocker.patch(
        "libsys_airflow.plugins.folio.invoices._folio_client",
        return_value=mock_folio_client,
    )
    invoice_ids = invoices_paid_within_date_range.function(
        dag_run=mock_scheduled_dag_run
    )
    assert len(invoice_ids) == 1
    assert invoice_ids[0] == "34cabbbd-d419-4853-ad3a-d0eafd4310c6"
    assert (
        f"Querying paid invoices with paymentDate range >= {mock_scheduled_dag_run.data_interval_start} and <= {mock_scheduled_dag_run.data_interval_end}"
        in caplog.text
    )


def test_no_invoices_paid_within_date_range(
    mocker, mock_folio_client_no_paid_invoices, mock_scheduled_dag_run, caplog
):
    mocker.patch(
        "libsys_airflow.plugins.folio.invoices._folio_client",
        return_value=mock_folio_client_no_paid_invoices,
    )
    invoice_ids = invoices_paid_within_date_range.function(
        dag_run=mock_scheduled_dag_run
    )
    assert len(invoice_ids['invoice_uuids']) == 0
    assert (
        f"NO PAID INVOICES between {mock_scheduled_dag_run.data_interval_start} and {mock_scheduled_dag_run.data_interval_end}. Downstream tasks will be skiped."
        in caplog.text
    )


def test_invoice_lines_from_invoices(mocker, mock_folio_client, caplog):
    mocker.patch(
        "libsys_airflow.plugins.folio.invoices._folio_client",
        return_value=mock_folio_client,
    )
    invoice_lines = invoice_lines_from_invoices.function(
        "29f339e3-dfdc-43e4-9442-eb817fdfb069"
    )
    assert (
        "Getting invoice lines for 29f339e3-dfdc-43e4-9442-eb817fdfb069" in caplog.text
    )
    assert len(invoice_lines) == 2
