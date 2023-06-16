import pytest  # noqa

from unittest.mock import MagicMock

from libsys_airflow.plugins.folio.orafin_payments import _get_invoice


@pytest.fixture
def mock_folio_client():
    def mock_get(*args, **kwargs):
        return {}

    mock_client = MagicMock()
    mock_client.get = mock_get
    return mock_client


def test_get_invoice(mock_folio_client):
    invoice = _get_invoice("a6452c96-53ef-4e51-bd7b-aa67ac971133", mock_folio_client)

    assert invoice["vendorId"] is None
    assert invoice["vat"] is False
