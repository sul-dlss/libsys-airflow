import pytest  # noqa

from unittest.mock import MagicMock

from libsys_airflow.plugins.folio.invoices import _get_ids_from_vouchers

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
