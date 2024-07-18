import pytest  # noqa

from unittest.mock import MagicMock

from libsys_airflow.plugins.folio.invoices import (
    invoices_awaiting_payment_task,
    _get_ids_from_vouchers,
)

vouchers = {"vouchers": [{"invoiceId": 'a6452c96-53ef-4e51-bd7b-aa67ac971133'}]}


@pytest.fixture
def mock_folio_client():
    def mock_get(*args, **kwargs):
        return vouchers

    mock_client = MagicMock()
    mock_client.get = mock_get
    return mock_client


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
