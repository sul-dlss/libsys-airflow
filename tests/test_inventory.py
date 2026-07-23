import pytest
import httpx
from unittest.mock import MagicMock
from datetime import datetime
from zoneinfo import ZoneInfo

from libsys_airflow.plugins.folio.inventory import add_admin_note_to_record
from folioclient import FolioError


@pytest.fixture(params=["instance", "holdings", "item"])
def mock_record(request):
    match request.param:
        case "instance":
            return {
                "record": {
                    "id": "e9603169-2808-4f8e-8c70-6aec354ac4fd",
                    "_version": 9,
                    "hrid": "in00000468660",
                    "source": "MARC",
                    "title": "A research agenda for Austrian economics / edited by Steven Horwitz, Louis Rouanet",
                    "series": [{"value": "Elgar research agendas."}],
                    "administrativeNotes": [
                        "GOBI",
                        "SUL/Acquisitions/MONOREC/nyoneda/20250804",
                        "SUL/DLSS/Libsys/bookplate/20260721",
                    ],
                },
                "endpoint": "/inventory/instances/e9603169-2808-4f8e-8c70-6aec354ac4fd",
            }
        case "holdings":
            return {
                "record": {
                    "id": "holdings-123",
                    "administrativeNotes": ["Holdings note"],
                },
                "endpoint": "/holdings-storage/holdings/holdings-123",
            }
        case "item":
            return {
                "record": {"id": "item-456", "administrativeNotes": ["Item note"]},
                "endpoint": "/inventory/items/item-456",
            }


@pytest.fixture
def mock_folio_client():
    mock_client = MagicMock()
    return mock_client


@pytest.mark.parametrize("mock_record", ["instance"], indirect=True)
def test_add_admin_note_to_instance(mocker, mock_folio_client, mock_record, caplog):
    mock_folio_client.folio_get.return_value = mock_record["record"]
    mock_folio_client.folio_put.return_value = httpx.Response(status_code=204)
    original_notes_count = len(mock_record["record"]["administrativeNotes"])
    mocker.patch(
        "libsys_airflow.plugins.folio.inventory.folio_client",
        return_value=mock_folio_client,
    )
    date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y%m%d")
    expected_endpoint = mock_record["endpoint"]
    result = add_admin_note_to_record(
        "SUL/DLSS/LibrarySystems/bookplate",
        "instance",
        "e9603169-2808-4f8e-8c70-6aec354ac4fd",
    )
    assert result is True
    assert (
        f"Adding admin note SUL/DLSS/LibrarySystems/bookplate with date {date} to instance"
        in caplog.text
    )
    mock_folio_client.folio_get.assert_called_once_with(expected_endpoint)
    mock_folio_client.folio_put.assert_called_once_with(
        expected_endpoint, mock_record["record"]
    )
    assert len(mock_record["record"]["administrativeNotes"]) == (
        original_notes_count + 1
    )


@pytest.mark.parametrize("mock_record", ["holdings"], indirect=True)
def test_add_admin_note_to_holdings(mocker, mock_folio_client, mock_record, caplog):
    mock_folio_client.folio_get.return_value = mock_record["record"]
    mock_folio_client.folio_put.return_value = httpx.Response(status_code=204)

    mocker.patch(
        "libsys_airflow.plugins.folio.inventory.folio_client",
        return_value=mock_folio_client,
    )
    date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y%m%d")
    expected_endpoint = mock_record["endpoint"]
    result = add_admin_note_to_record(
        "SUL/DLSS/LibrarySystems/BWcreatedby/sunetid", "holdings", "holdings-123"
    )
    assert result is True
    assert (
        f"Adding admin note SUL/DLSS/LibrarySystems/BWcreatedby/sunetid with date {date} to holdings"
        in caplog.text
    )
    mock_folio_client.folio_get.assert_called_once_with(expected_endpoint)
    mock_folio_client.folio_put.assert_called_once_with(
        expected_endpoint, mock_record["record"]
    )


@pytest.mark.parametrize("mock_record", ["item"], indirect=True)
def test_add_admin_note_to_item(mocker, mock_folio_client, mock_record, caplog):
    mock_folio_client.folio_get.return_value = mock_record["record"]
    mock_folio_client.folio_put.return_value = httpx.Response(status_code=204)

    mocker.patch(
        "libsys_airflow.plugins.folio.inventory.folio_client",
        return_value=mock_folio_client,
    )
    date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y%m%d")
    expected_endpoint = mock_record["endpoint"]
    result = add_admin_note_to_record(
        "SUL/DLSS/LibrarySystems/BWcreatedby/sunetid", "item", "item-456"
    )
    assert result is True
    assert (
        f"Adding admin note SUL/DLSS/LibrarySystems/BWcreatedby/sunetid with date {date} to item"
        in caplog.text
    )
    mock_folio_client.folio_get.assert_called_once_with(expected_endpoint)
    mock_folio_client.folio_put.assert_called_once_with(
        expected_endpoint, mock_record["record"]
    )


@pytest.mark.parametrize("mock_record", ["instance"], indirect=True)
def test_adds_new_note(mocker, mock_folio_client, mock_record):
    mock_instance = mock_record["record"]
    mock_folio_client.folio_get.return_value = mock_instance
    mock_folio_client.folio_put.return_value = httpx.Response(status_code=204)

    mocker.patch(
        "libsys_airflow.plugins.folio.inventory.folio_client",
        return_value=mock_folio_client,
    )
    date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y%m%d")
    original_notes_count = len(mock_instance["administrativeNotes"])
    result = add_admin_note_to_record(
        "NEW/Note/Type", "instance", "e9603169-2808-4f8e-8c70-6aec354ac4fd"
    )
    assert result is True
    assert len(mock_instance["administrativeNotes"]) == original_notes_count + 1
    assert f"NEW/Note/Type/{date}" in mock_instance["administrativeNotes"]


@pytest.mark.parametrize("mock_record", ["instance"], indirect=True)
def test_updates_existing_note_with_date(
    mocker, mock_folio_client, mock_record, caplog
):
    mock_instance = mock_record["record"]
    mock_folio_client.folio_get.return_value = mock_instance
    mock_folio_client.folio_put.return_value = httpx.Response(status_code=204)

    mocker.patch(
        "libsys_airflow.plugins.folio.inventory.folio_client",
        return_value=mock_folio_client,
    )
    original_notes_count = len(mock_instance["administrativeNotes"])
    date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y%m%d")
    result = add_admin_note_to_record(
        "SUL/DLSS/Libsys/bookplate", "instance", "e9603169-2808-4f8e-8c70-6aec354ac4fd"
    )
    assert result is True
    assert (
        f"Adding admin note SUL/DLSS/Libsys/bookplate with date {date} to instance"
        in caplog.text
    )
    # Check that the existing note was updated with the new date
    updated_notes = mock_instance["administrativeNotes"]
    assert f"SUL/DLSS/Libsys/bookplate/{date}" in updated_notes
    # The note should have updated an existing note
    assert len(updated_notes) == original_notes_count


def test_handles_empty_administrative_notes(mocker, mock_folio_client):
    mock_record = {"id": "test-id", "administrativeNotes": []}
    mock_folio_client.folio_get.return_value = mock_record
    mock_folio_client.folio_put.return_value = httpx.Response(status_code=204)

    mocker.patch(
        "libsys_airflow.plugins.folio.inventory.folio_client",
        return_value=mock_folio_client,
    )
    date = datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y%m%d")
    result = add_admin_note_to_record("test note", "instance", "test-id")

    assert result is True
    assert mock_record["administrativeNotes"] == [f"test note/{date}"]


def test_handles_folio_error(mocker, mock_folio_client, caplog):
    mock_record = {"id": "test-id", "administrativeNotes": []}
    mock_folio_client.folio_get.return_value = mock_record
    mock_folio_client.folio_put.side_effect = FolioError("Connection failed")

    mocker.patch(
        "libsys_airflow.plugins.folio.inventory.folio_client",
        return_value=mock_folio_client,
    )

    result = add_admin_note_to_record("test note", "instance", "test-id")

    assert result is False
    assert "Adding admin note failed: Connection failed" in caplog.text
