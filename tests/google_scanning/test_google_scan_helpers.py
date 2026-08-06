import pytest

from libsys_airflow.plugins.google_scanning.helpers import (
    _lookup_item_by_barcode,
    _update_item_for_shipment,
    process_barcode,
    read_staged_barcode_files,
)

FOUND_BARCODE = "36105000000000"
NOT_FOUND_BARCODE = "00000000000000"
MULTIPLE_BARCODE = "11111111111111"
ERROR_BARCODE = "99999999999999"

ITEM_ID = "de17bd82-7ba7-4dc7-a4e0-1e28e6f4b5c7"
TEMP_LOCATION_ID = "5f5f5f5f-6666-7777-8888-999999999999"
DIGI_SENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
NOTE_TYPE_ID = "11112222-3333-4444-5555-666677778888"


def _item(**overrides) -> dict:
    item = {
        "id": ITEM_ID,
        "barcode": FOUND_BARCODE,
        "statisticalCodeIds": [],
        "notes": [],
    }
    item.update(overrides)
    return item


@pytest.fixture
def mock_folio_client(mocker):
    def mock_get(*args, **kwargs):
        query = kwargs.get("query", "")
        barcode = query.split("==")[-1]
        match barcode:
            case b if b == FOUND_BARCODE:
                return [_item()]
            case b if b == MULTIPLE_BARCODE:
                return [_item(), _item()]
            case b if b == ERROR_BARCODE:
                raise ValueError("500: Server Error")
            case _:
                return []

    mock_client = mocker.MagicMock()
    mock_client.folio_get = mocker.Mock(side_effect=mock_get)
    mock_client.folio_put = mocker.Mock()
    return mock_client


def test_barcode_found(mock_folio_client):
    result = _lookup_item_by_barcode(FOUND_BARCODE, mock_folio_client)

    assert result["id"] == ITEM_ID


def test_barcode_not_found(mock_folio_client):
    result = _lookup_item_by_barcode(NOT_FOUND_BARCODE, mock_folio_client)

    assert result == {"error": f"not found barcode: {NOT_FOUND_BARCODE}"}


def test_multiple_barcodes_found(mock_folio_client):
    result = _lookup_item_by_barcode(MULTIPLE_BARCODE, mock_folio_client)

    assert result == {"error": f"multiple items found for barcode: {MULTIPLE_BARCODE}"}


def test_folio_get_barcodes_raises(mock_folio_client):
    result = _lookup_item_by_barcode(ERROR_BARCODE, mock_folio_client)

    assert f"for barcode: {ERROR_BARCODE}" in result["error"]
    assert "500: Server Error" in result["error"]


def test_sets_temp_location_stat_code_and_note(mock_folio_client):
    item = _item()

    result = _update_item_for_shipment(
        item=item,
        folio_client=mock_folio_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
        date="2026-08-06",
    )

    assert result == {}
    assert item["temporaryLocationId"] == TEMP_LOCATION_ID
    assert item["statisticalCodeIds"] == [DIGI_SENT_ID]
    assert item["notes"] == [
        {
            "itemNoteTypeId": NOTE_TYPE_ID,
            "note": "Sent to Google on 2026-08-06",
            "staffOnly": True,
        }
    ]
    mock_folio_client.folio_put.assert_called_once_with(
        f"/inventory/items/{ITEM_ID}", payload=item
    )


def test_does_not_duplicate_existing_stat_code(mock_folio_client):
    item = _item(statisticalCodeIds=[DIGI_SENT_ID])

    _update_item_for_shipment(
        item=item,
        folio_client=mock_folio_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
        date="2026-08-06",
    )

    assert item["statisticalCodeIds"] == [DIGI_SENT_ID]


def test_preserves_existing_stat_codes(mock_folio_client):
    existing_stat_code = "aaaa1111-bbbb-2222-cccc-333344445555"
    item = _item(statisticalCodeIds=[existing_stat_code])

    _update_item_for_shipment(
        item=item,
        folio_client=mock_folio_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
        date="2026-08-06",
    )

    assert item["statisticalCodeIds"] == [existing_stat_code, DIGI_SENT_ID]


def test_folio_put_error_reports_id_and_barcode(mocker):
    item = _item()
    mock_client = mocker.MagicMock()
    mock_client.folio_put = mocker.Mock(side_effect=ValueError("500: Server Error"))

    result = _update_item_for_shipment(
        item=item,
        folio_client=mock_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
        date="2026-08-06",
    )

    assert item["id"] in result["error"]
    assert item["barcode"] in result["error"]
    assert "500: Server Error" in result["error"]


def test_success_updates_item(mock_folio_client):
    result = process_barcode(
        barcode=FOUND_BARCODE,
        folio_client=mock_folio_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
    )

    assert result == {}
    mock_folio_client.folio_put.assert_called_once()


def test_not_found_short_circuits_before_update(mock_folio_client):
    result = process_barcode(
        barcode=NOT_FOUND_BARCODE,
        folio_client=mock_folio_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
    )

    assert result == {"error": f"not found barcode: {NOT_FOUND_BARCODE}"}
    mock_folio_client.folio_put.assert_not_called()


def test_multiple_found_short_circuits_before_update(mock_folio_client):
    result = process_barcode(
        barcode=MULTIPLE_BARCODE,
        folio_client=mock_folio_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
    )

    assert "multiple items found" in result["error"]
    mock_folio_client.folio_put.assert_not_called()


def test_uses_current_date_in_note(mock_folio_client, mocker):
    mock_datetime = mocker.patch(
        "libsys_airflow.plugins.google_scanning.helpers.datetime"
    )
    mock_datetime.datetime.now.return_value.strftime.return_value = "2026-08-06"

    item = _item()
    process_barcode(
        barcode=FOUND_BARCODE,
        folio_client=mock_folio_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
    )

    updated_item = mock_folio_client.folio_put.call_args.kwargs["payload"]
    assert updated_item["notes"][0]["note"] == "Sent to Google on 2026-08-06"


def test_read_staged_barcode_files_returns_barcodes(tmp_path):
    barcode_file = tmp_path / "cart-1.txt"
    barcode_file.write_text(f"{FOUND_BARCODE}\n{MULTIPLE_BARCODE}\n")

    result = read_staged_barcode_files(str(barcode_file))

    assert result == [FOUND_BARCODE, MULTIPLE_BARCODE]


def test_read_staged_barcode_files_strips_whitespace(tmp_path):
    barcode_file = tmp_path / "cart-2.txt"
    barcode_file.write_text(f"  {FOUND_BARCODE}  \n\t{MULTIPLE_BARCODE}\t\n")

    result = read_staged_barcode_files(str(barcode_file))

    assert result == [FOUND_BARCODE, MULTIPLE_BARCODE]


def test_read_staged_barcode_files_skips_blank_lines(tmp_path):
    barcode_file = tmp_path / "cart-3.txt"
    barcode_file.write_text(f"{FOUND_BARCODE}\n\n   \n{MULTIPLE_BARCODE}\n")

    result = read_staged_barcode_files(str(barcode_file))

    assert result == [FOUND_BARCODE, MULTIPLE_BARCODE]


def test_read_staged_barcode_files_raises_when_missing(tmp_path):
    missing_file = tmp_path / "does-not-exist.txt"

    with pytest.raises(FileNotFoundError, match="does not exist"):
        read_staged_barcode_files(str(missing_file))
