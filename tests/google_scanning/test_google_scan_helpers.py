import json
import logging

import pytest

from folioclient import FolioDataConflictError

from libsys_airflow.plugins.google_scanning.helpers import (
    _lookup_item_by_barcode,
    _update_item_for_staging,
    get_folio_uuids,
    parse_barcodes,
    process_barcode,
    read_staged_barcode_files,
    write_status_json,
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


def _conflict_error(mocker) -> FolioDataConflictError:
    return FolioDataConflictError(
        "optimistic locking failure",
        request=mocker.Mock(),
        response=mocker.Mock(status_code=409),
    )


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

    assert result == {"missing": NOT_FOUND_BARCODE}


def test_multiple_barcodes_found(mock_folio_client):
    result = _lookup_item_by_barcode(MULTIPLE_BARCODE, mock_folio_client)

    assert result == {
        "barcode": MULTIPLE_BARCODE,
        "reason": f"multiple items found for barcode: {MULTIPLE_BARCODE}",
    }


def test_folio_get_barcodes_raises(mock_folio_client):
    result = _lookup_item_by_barcode(ERROR_BARCODE, mock_folio_client)

    assert result["barcode"] == ERROR_BARCODE
    assert f"for barcode: {ERROR_BARCODE}" in result["reason"]
    assert "500: Server Error" in result["reason"]


def test_sets_temp_location_stat_code_and_note(mock_folio_client):
    item = _item()

    result = _update_item_for_staging(
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
            "note": "Sent for Google scanning/2026-08-06",
            "staffOnly": True,
        }
    ]
    mock_folio_client.folio_put.assert_called_once_with(
        f"/inventory/items/{ITEM_ID}", payload=item
    )


def test_does_not_duplicate_existing_stat_code(mock_folio_client):
    item = _item(statisticalCodeIds=[DIGI_SENT_ID])

    _update_item_for_staging(
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

    _update_item_for_staging(
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

    result = _update_item_for_staging(
        item=item,
        folio_client=mock_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
        date="2026-08-06",
    )

    assert result["barcode"] == item["barcode"]
    assert item["id"] in result["reason"]
    assert "500: Server Error" in result["reason"]


def test_retries_once_and_succeeds_after_conflict(mock_folio_client, mocker):
    item = _item()
    mock_folio_client.folio_put = mocker.Mock(
        side_effect=[_conflict_error(mocker), None]
    )

    result = _update_item_for_staging(
        item=item,
        folio_client=mock_folio_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
        date="2026-08-06",
    )

    assert result == {}
    assert mock_folio_client.folio_put.call_count == 2
    retried_item = mock_folio_client.folio_put.call_args.kwargs["payload"]
    assert retried_item["statisticalCodeIds"] == [DIGI_SENT_ID]


def test_reports_error_when_retry_also_conflicts(mock_folio_client, mocker):
    item = _item()
    mock_folio_client.folio_put = mocker.Mock(
        side_effect=[_conflict_error(mocker), _conflict_error(mocker)]
    )

    result = _update_item_for_staging(
        item=item,
        folio_client=mock_folio_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
        date="2026-08-06",
    )

    assert result["barcode"] == item["barcode"]
    assert item["id"] in result["reason"]
    assert mock_folio_client.folio_put.call_count == 2


def test_reports_error_when_refetch_finds_nothing(mocker):
    item = _item()
    mock_client = mocker.MagicMock()
    mock_client.folio_get = mocker.Mock(return_value=[])
    mock_client.folio_put = mocker.Mock(side_effect=_conflict_error(mocker))

    result = _update_item_for_staging(
        item=item,
        folio_client=mock_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
        date="2026-08-06",
    )

    assert result["barcode"] == item["barcode"]
    assert item["id"] in result["reason"]
    mock_client.folio_put.assert_called_once()


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

    assert result == {"missing": NOT_FOUND_BARCODE}
    mock_folio_client.folio_put.assert_not_called()


def test_multiple_found_short_circuits_before_update(mock_folio_client):
    result = process_barcode(
        barcode=MULTIPLE_BARCODE,
        folio_client=mock_folio_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
    )

    assert "multiple items found" in result["reason"]
    mock_folio_client.folio_put.assert_not_called()


def test_uses_current_date_in_note(mock_folio_client, mocker):
    mock_datetime = mocker.patch(
        "libsys_airflow.plugins.google_scanning.helpers.datetime"
    )
    mock_datetime.datetime.now.return_value.strftime.return_value = "2026-08-06"

    process_barcode(
        barcode=FOUND_BARCODE,
        folio_client=mock_folio_client,
        temp_location_id=TEMP_LOCATION_ID,
        digi_sent_id=DIGI_SENT_ID,
        note_type_id=NOTE_TYPE_ID,
    )

    updated_item = mock_folio_client.folio_put.call_args.kwargs["payload"]
    assert updated_item["notes"][0]["note"] == "Sent for Google scanning/2026-08-06"


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


def test_parse_barcodes_strips_whitespace_and_skips_blank_lines():
    result = parse_barcodes(f"  {FOUND_BARCODE}  \n\n   \n\t{MULTIPLE_BARCODE}\t\n")

    assert result == [FOUND_BARCODE, MULTIPLE_BARCODE]


LOCATION_CODE = "SUL-SEE-OTHER"
LOCATION_UUID = "11111111-2222-3333-4444-555555555555"
DIGI_SENT_STAT_UUID = "66666666-7777-8888-9999-000000000000"
ITEM_NOTE_TYPE_UUID = "aaaaaaaa-1111-2222-3333-444444444444"


def _mock_client_for_uuids(mocker, locations, statistical_codes, item_note_types):
    mock_client = mocker.MagicMock()
    mock_client.locations = locations
    mock_client.statistical_codes = statistical_codes
    mock_client.folio_get = mocker.Mock(return_value=item_note_types)
    return mock_client


def test_get_folio_uuids_returns_all_three_ids(mocker):
    mock_client = _mock_client_for_uuids(
        mocker,
        locations=[{"code": LOCATION_CODE, "id": LOCATION_UUID}],
        statistical_codes=[{"code": "DIGI-SENT", "id": DIGI_SENT_STAT_UUID}],
        item_note_types=[{"id": ITEM_NOTE_TYPE_UUID}],
    )

    result = get_folio_uuids(mock_client, LOCATION_CODE)

    assert result == (LOCATION_UUID, DIGI_SENT_STAT_UUID, ITEM_NOTE_TYPE_UUID)
    mock_client.folio_get.assert_called_once_with(
        "/item-note-types", key="itemNoteTypes", query="name==Reproduction"
    )


def test_get_folio_uuids_matches_location_code_prefix(mocker):
    mock_client = _mock_client_for_uuids(
        mocker,
        locations=[{"code": f"{LOCATION_CODE}-1", "id": LOCATION_UUID}],
        statistical_codes=[{"code": "DIGI-SENT", "id": DIGI_SENT_STAT_UUID}],
        item_note_types=[{"id": ITEM_NOTE_TYPE_UUID}],
    )

    result = get_folio_uuids(mock_client, LOCATION_CODE)

    assert result == (LOCATION_UUID, DIGI_SENT_STAT_UUID, ITEM_NOTE_TYPE_UUID)


def test_get_folio_uuids_raises_when_location_code_missing(mocker):
    mock_client = _mock_client_for_uuids(
        mocker,
        locations=[{"code": "OTHER-CODE", "id": LOCATION_UUID}],
        statistical_codes=[{"code": "DIGI-SENT", "id": DIGI_SENT_STAT_UUID}],
        item_note_types=[{"id": ITEM_NOTE_TYPE_UUID}],
    )

    with pytest.raises(ValueError, match="Missing temp location code"):
        get_folio_uuids(mock_client, LOCATION_CODE)


def test_get_folio_uuids_raises_when_multiple_location_codes_match(mocker):
    mock_client = _mock_client_for_uuids(
        mocker,
        locations=[
            {"code": LOCATION_CODE, "id": LOCATION_UUID},
            {"code": f"{LOCATION_CODE}-2", "id": "other-uuid"},
        ],
        statistical_codes=[{"code": "DIGI-SENT", "id": DIGI_SENT_STAT_UUID}],
        item_note_types=[{"id": ITEM_NOTE_TYPE_UUID}],
    )

    with pytest.raises(ValueError, match="too many locations codes"):
        get_folio_uuids(mock_client, LOCATION_CODE)


def test_get_folio_uuids_raises_when_digi_sent_stat_code_missing(mocker):
    mock_client = _mock_client_for_uuids(
        mocker,
        locations=[{"code": LOCATION_CODE, "id": LOCATION_UUID}],
        statistical_codes=[{"code": "DIGI-SDR", "id": "other-uuid"}],
        item_note_types=[{"id": ITEM_NOTE_TYPE_UUID}],
    )

    with pytest.raises(ValueError, match="Missing DIGI-SENT stat code"):
        get_folio_uuids(mock_client, LOCATION_CODE)


def test_get_folio_uuids_raises_when_multiple_digi_sent_stat_codes_match(mocker):
    mock_client = _mock_client_for_uuids(
        mocker,
        locations=[{"code": LOCATION_CODE, "id": LOCATION_UUID}],
        statistical_codes=[
            {"code": "DIGI-SENT", "id": DIGI_SENT_STAT_UUID},
            {"code": "DIGI-SENT-2", "id": "other-uuid"},
        ],
        item_note_types=[{"id": ITEM_NOTE_TYPE_UUID}],
    )

    with pytest.raises(ValueError, match="Missing DIGI-SENT stat code"):
        get_folio_uuids(mock_client, LOCATION_CODE)


def test_get_folio_uuids_raises_when_item_note_type_missing(mocker):
    mock_client = _mock_client_for_uuids(
        mocker,
        locations=[{"code": LOCATION_CODE, "id": LOCATION_UUID}],
        statistical_codes=[{"code": "DIGI-SENT", "id": DIGI_SENT_STAT_UUID}],
        item_note_types=[],
    )

    with pytest.raises(ValueError, match="Missing Item Note types"):
        get_folio_uuids(mock_client, LOCATION_CODE)


def test_get_folio_uuids_raises_when_multiple_item_note_types_match(mocker):
    mock_client = _mock_client_for_uuids(
        mocker,
        locations=[{"code": LOCATION_CODE, "id": LOCATION_UUID}],
        statistical_codes=[{"code": "DIGI-SENT", "id": DIGI_SENT_STAT_UUID}],
        item_note_types=[{"id": ITEM_NOTE_TYPE_UUID}, {"id": "other-uuid"}],
    )

    with pytest.raises(ValueError, match="Missing Item Note types"):
        get_folio_uuids(mock_client, LOCATION_CODE)


def _mock_now(mocker, isoformat_value):
    mock_datetime = mocker.patch(
        "libsys_airflow.plugins.google_scanning.helpers.datetime"
    )
    mock_datetime.datetime.now.return_value.isoformat.return_value = isoformat_value
    return mock_datetime


def test_write_status_json_writes_status_fields(tmp_path, mocker):
    _mock_now(mocker, "2026-08-07T00:00:00+00:00")
    barcode_file = tmp_path / "cart-4.txt"
    barcode_file.write_text(f"{FOUND_BARCODE}\n")
    init_params = {"staged_file_path": str(barcode_file), "cart_name": "Cart 1"}
    update_results = {
        "successful_updates": 1,
        "missing": [NOT_FOUND_BARCODE],
        "errors": [],
    }

    result = write_status_json(init_params, 2, update_results)

    assert result is True
    status = json.loads((tmp_path / "status.json").read_text())
    assert status == {
        "cart_name": "Cart 1",
        "staged_at": "2026-08-07T00:00:00+00:00",
        "total_barcodes": 2,
        "updated": 1,
        "missing_barcodes": [NOT_FOUND_BARCODE],
        "errors": [],
        "status": "staged",
        "shipped_at": None,
        "shipment_dag_run_id": None,
    }


def test_write_status_json_marks_failed_when_nothing_updated(tmp_path, mocker):
    _mock_now(mocker, "2026-08-07T00:00:00+00:00")
    barcode_file = tmp_path / "cart-9.txt"
    barcode_file.write_text(f"{NOT_FOUND_BARCODE}\n")
    init_params = {"staged_file_path": str(barcode_file), "cart_name": "Cart 9"}
    update_results = {
        "successful_updates": 0,
        "missing": [NOT_FOUND_BARCODE],
        "errors": [],
    }

    write_status_json(init_params, 1, update_results)

    status = json.loads((tmp_path / "status.json").read_text())
    assert status["status"] == "failed"


def test_write_status_json_writes_next_to_staged_file(tmp_path, mocker):
    _mock_now(mocker, "2026-08-07T00:00:00+00:00")
    nested_dir = tmp_path / "cart-5"
    nested_dir.mkdir()
    barcode_file = nested_dir / "cart-5.txt"
    barcode_file.write_text(f"{FOUND_BARCODE}\n")
    init_params = {"staged_file_path": str(barcode_file), "cart_name": "Cart 5"}
    update_results = {"successful_updates": [], "missing": [], "errors": []}

    write_status_json(init_params, 0, update_results)

    assert (nested_dir / "status.json").exists()
    assert not (tmp_path / "status.json").exists()


def test_write_status_json_overwrites_existing_file(tmp_path, mocker):
    _mock_now(mocker, "2026-08-07T00:00:00+00:00")
    barcode_file = tmp_path / "cart-7.txt"
    barcode_file.write_text(f"{FOUND_BARCODE}\n")
    status_json_path = tmp_path / "status.json"
    status_json_path.write_text(json.dumps({"stale": "data"}))
    init_params = {"staged_file_path": str(barcode_file), "cart_name": "Cart 7"}
    update_results = {
        "successful_updates": 1,
        "missing": [],
        "errors": [],
    }

    write_status_json(init_params, 1, update_results)

    status = json.loads(status_json_path.read_text())
    assert status["updated"] == 1
    assert "stale" not in status


def test_write_status_json_returns_false_and_logs_on_error(tmp_path, mocker, caplog):
    _mock_now(mocker, "2026-08-07T00:00:00+00:00")
    barcode_file = tmp_path / "cart-8.txt"
    barcode_file.write_text(f"{FOUND_BARCODE}\n")
    mocker.patch(
        "libsys_airflow.plugins.google_scanning.helpers.json.dump",
        side_effect=TypeError("not serializable"),
    )
    init_params = {"staged_file_path": str(barcode_file), "cart_name": "Cart 8"}
    update_results = {"successful_updates": 0, "missing": [], "errors": []}

    with caplog.at_level(logging.ERROR):
        result = write_status_json(init_params, 0, update_results)

    assert result is False
    assert "not serializable" in caplog.text
