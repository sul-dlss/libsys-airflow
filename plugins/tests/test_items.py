import json

import pandas as pd
import pytest  # noqa
import requests

from copy import deepcopy

from pytest_mock import MockerFixture
from plugins.tests.mocks import mock_dag_run, mock_file_system, MockFOLIOClient  # noqa


from plugins.folio.items import (
    post_folio_items_records,
    run_items_transformer,
    _add_additional_info,
    _generate_item_notes,
    _suppressed_conditions,
)


def test_post_folio_items_records():
    assert post_folio_items_records


def test_items_transformers():
    assert run_items_transformer


item_note_types = {
    "itemNoteTypes": [
        {"id": "8d0a5eca-25de-4391-81a9-236eeefdd20b", "name": "Note"},
        {"id": "e9f6de86-e564-4095-a61a-38c9e0e6b2fc", "name": "Tech Staff"},
        {"id": "62fd6fcc-5cde-4a74-849a-66e2d77a1f12", "name": "Public"},
        {"id": "1d14675c-c163-4502-98f9-961cd3d17ab2", "name": "Circ Staff"},
    ]
}

statistical_code_types = {
    'statisticalCodeTypes': [
        {
            'id': '6c126b3a-3859-451c-8576-15b26b205d43',
            'name': 'Item'
        }
    ]
}

statistical_codes = {
    'statisticalCodes': [
        {
            'id': '8be8d577-1cd7-4b84-ae71-d9472fc4d2b1',
            'code': 'DIGI-SENT'
        },
        {
            'id': '9c98fbcc-1728-41f5-9382-038d9fa45c0f',
            'code': 'FED-WEED'
        }
    ]
}


@pytest.fixture
def mock_okapi_items_endpoint(monkeypatch, mocker: MockerFixture):  # noqa
    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get_result")
        get_response.status_code = 200
        endpoint = args[0].split("/")[-1]
        match endpoint:
            case 'item-note-types?limit=100':
                get_response.json = lambda: item_note_types

            case 'statistical-code-types?query=name==Item&limit=200':
                get_response.json = lambda: statistical_code_types

            case _:
                get_response.json = lambda: statistical_codes

        get_response.raise_for_status = ValueError
        return get_response

    monkeypatch.setattr(requests, "get", mock_get)


holdings_recs = [
    {
        "id": "8e6e9fb5-f914-4d38-87d2-ccb52f9a44a4",
        "formerIds": ["a23456"],
        "hrid": "ah23456_1",
    },
    {
        "hrid": "ah9704208_1",
        "id": "b9cd36d8-a031-5793-b2e8-42042cc2dade",
        "formerIds": ["a9704208"],
    },
]

items_notes = [
    "BARCODE\tnote\tNOTE_TYPE\n",
    "1233455\ta note\tCIRCSTAFF\n",
    "1233455\ta note for the public\tCIRCNOTE\n",
    "55678446243\tavailable for checkout\tPUBLIC\n",
    "55678446243\ttf:green, hbr 9/20/2013\tTECHSTAFF",
]

items_recs = [
    {
        "holdingsRecordId": "8e6e9fb5-f914-4d38-87d2-ccb52f9a44a4",
        "barcode": "55678446243",
    },
    {
        "holdingsRecordId": "b9cd36d8-a031-5793-b2e8-42042cc2dade",
        "barcode": "1233455",
    },
    {
        "holdingsRecordId": "a3a494c2-2af3-4afd-9cf9-666c4052cef9",
        "barcode": "4614642357",
    },
    {
        "holdingsRecordId": "9c262b9a-532c-4f48-8fcb-e126dac04300",
        "barcode": "7659908473",
    },
    {
        "holdingsRecordId": "fc473c74-c811-4ae9-bcd9-387a1d10b967",
        "barcode": "0267132027",
    },
]

items_tsv = [
    "CATKEY\tCALL_SEQ\tCOPY\tBARCODE\tLIBRARY\tHOMELOCATION\tCURRENTLOCATION\tITEM_TYPE\tITEM_CAT1\tITEM_CAT2\tITEM_SHADOW\tCALL_NUMBER_TYPE\tBASE_CALL_NUMBER\tVOLUME_INFO\tCALL_SHADOW\tFORMAT\tCATALOG_SHADOW",
    "23456\t1\t1\t55678446243\tSAL3\tINPROCESS\tCOLDSTOR\tSTKS-MONO\tDIGI-SENT\t\t0\tLC\tTR640 .I34 1996\t\t0\tMARC\t0",
    "9704208\t1\t1\t1233455\tSAL3\tINPROCESS\tINPROCESS\tSTKS-MONO\t\t\t0\tLC\tTR640 .I34 1996\t\t0\tMARC\t0",
    "145623\t1\t1\t4614642357\tSAL3\tSPECB-S\tINPROCESS\tSTKS-MONO\t\tFED-WEED\t0\tLC\tRH640 .I34 1996\t\t0\tMARC\t0",
    "262345\t1\t1\t7659908473\tSAL3\tINPROCESS\tINPROCESS\tSTKS-MONO\t\t\t1\tLC\tYU40 .J4 2096\t\t0\tMARC\t0",  # ITEM SHADOW
    "5559991\t1\t1\t7659908473\tSAL3\tINPROCESS\tINPROCESS\tSTKS-MONO\tDIGI-SENT\tFED-WEED\t0\tLC\tEG640 .J4 1796\t\t1\tMARC\t0",  # CALL SHADOW
]


def setup_items_holdings(
    airflow_dir,
    results_dir,
    iteration_dir,
    items_recs=items_recs,
    items_notes=items_notes,
    holdings_recs=holdings_recs,
):
    holdings_path = results_dir / "holdings_transformer-test_dag.json"

    with holdings_path.open("w+") as fo:
        for rec in holdings_recs:
            fo.write(f"{json.dumps(rec)}\n")

    items_path = results_dir / "items_transformer-test_dag.json"

    with items_path.open("w+") as fo:
        for rec in items_recs:
            fo.write(f"{json.dumps(rec)}\n")

    item_tsv_source_dir = iteration_dir / "source_data/items"

    with (item_tsv_source_dir / "ckey_001_002.tsv").open("w+") as fo:
        for row in items_tsv:
            fo.write(f"{row}\n")

    (item_tsv_source_dir / "ckey_001_002.electronic.tsv").touch()

    suppressed_locations_path = airflow_dir / "migration/mapping_files/items-suppressed-locations.json"

    with suppressed_locations_path.open("w+") as fo:
        json.dump([
            "COLDSTOR",
            "SPECB-S"
        ], fo)

    stat_codes_path = airflow_dir / "migration/mapping_files/statcodes.tsv"

    with stat_codes_path.open("w+") as fo:
        for row in ["ITEM_CATS\tfolio_code",
                    "DIGI-SENT\tDIGI-SENT",
                    "FED-WEED\tFED-WEED"]:
            fo.write(f"{row}\n")

    data_prep = iteration_dir / "data_preparation/"

    data_prep.mkdir(parents=True)

    items_notes_path = data_prep / "test_dag.notes.tsv"

    items_notes_path.write_text("".join(items_notes))

    return items_path, items_notes_path


def test_add_additional_info(
    mock_file_system, mock_dag_run, mock_okapi_items_endpoint  # noqa
):
    airflow_path = mock_file_system[0]
    iteration_dir = mock_file_system[2]
    results_dir = mock_file_system[3]

    items_path, items_notes_path = setup_items_holdings(airflow_path, results_dir, iteration_dir)

    folio_client = MockFOLIOClient()

    _add_additional_info(
        airflow=str(airflow_path),
        dag_run_id=mock_dag_run.run_id,
        items_tsv="ckey_001_002.tsv",
        items_pattern="items_transformer-*.json",
        tsv_notes_path=items_notes_path,
        folio_client=folio_client,
    )

    with items_path.open() as items_fo:
        new_items_recs = [json.loads(row) for row in items_fo.readlines()]

    assert new_items_recs[0]["_version"] == 1
    assert new_items_recs[0]["notes"][0]["staffOnly"] is False
    assert new_items_recs[0]["notes"][0]["note"] == "available for checkout"
    assert (
        new_items_recs[0]["notes"][0]["itemNoteTypeId"]
        == "62fd6fcc-5cde-4a74-849a-66e2d77a1f12"
    )
    assert new_items_recs[0]["notes"][1]["staffOnly"]
    assert new_items_recs[0]["discoverySuppress"] is True

    assert (
        new_items_recs[0]["notes"][1]["itemNoteTypeId"]
        == "e9f6de86-e564-4095-a61a-38c9e0e6b2fc"
    )

    assert (
        new_items_recs[0]["statisticalCodeIds"][0]
        == "8be8d577-1cd7-4b84-ae71-d9472fc4d2b1"
    )

    assert new_items_recs[1]["notes"][0]["staffOnly"]
    assert (
        new_items_recs[1]["notes"][0]["itemNoteTypeId"]
        == "1d14675c-c163-4502-98f9-961cd3d17ab2"
    )
    assert new_items_recs[1]["notes"][1]["staffOnly"]
    assert "discoverySuppress" not in new_items_recs[1]
    assert new_items_recs[2]["discoverySuppress"] is True
    assert new_items_recs[3]["discoverySuppress"] is True
    assert (
        new_items_recs[-2]["statisticalCodeIds"]
        == ["8be8d577-1cd7-4b84-ae71-d9472fc4d2b1",
            "9c98fbcc-1728-41f5-9382-038d9fa45c0f"]
    )


def test_add_additional_info_missing_barcode(
    mock_file_system, mock_dag_run, mock_okapi_items_endpoint  # noqa
):
    global items_recs
    airflow_dir = mock_file_system[0]
    iteration_dir = mock_file_system[2]
    results_dir = mock_file_system[3]

    items_recs_copy = deepcopy(items_recs)

    items_recs[0].pop("barcode")

    items_path, items_notes_path = setup_items_holdings(
        airflow_dir,
        results_dir,
        iteration_dir,
        items_recs
    )

    folio_client = MockFOLIOClient()

    _add_additional_info(
        airflow=str(mock_file_system[0]),
        dag_run_id=mock_dag_run.run_id,
        items_tsv="ckey_001_002.tsv",
        holdings_pattern="holdings_transformer-*.json",
        items_pattern="items_transformer-*.json",
        tsv_notes_path=items_notes_path,
        folio_client=folio_client,
    )

    with items_path.open() as items_fo:
        new_items_recs = [json.loads(row) for row in items_fo.readlines()]

    assert new_items_recs[0]["_version"] == 1

    assert "barcode" not in new_items_recs[0]

    items_recs = items_recs_copy


def test_add_additional_info_hoover(mock_file_system, mock_okapi_items_endpoint):  # noqa
    pass


def test_generate_item_notes_missing_barcode(caplog):  # noqa
    item = {"hrid": "ai123455"}
    tsv_notes_df = pd.DataFrame()
    _generate_item_notes(item, tsv_notes_df, {})

    assert "Item missing barcode, cannot generate notes" in caplog.text


def test_suppressed_conditions_missing_item_tsv(
    mock_file_system, mock_dag_run, caplog  # noqa
):
    _suppressed_conditions(mock_file_system[0], mock_dag_run.run_id)

    assert "No item tsv file found" in caplog.text
