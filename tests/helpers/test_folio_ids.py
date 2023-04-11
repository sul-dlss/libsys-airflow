import json

import pydantic

from libsys_airflow.plugins.folio.helpers.folio_ids import (
    generate_holdings_identifiers,
    generate_item_identifiers,
)

from tests.test_holdings import holdings as mock_holding_records
from tests.test_holdings import instances_holdings_items_map

from tests.mocks import mock_dag_run, mock_file_system, mock_okapi_variable  # noqa


def test_generate_holdings_identifiers(
    mock_file_system, mock_dag_run, mock_okapi_variable  # noqa
):
    airflow = mock_file_system[0]
    results_dir = mock_file_system[3]

    instance_filepath = results_dir / "folio_instances_bibs-transformer.json"

    with instance_filepath.open("w+") as fo:
        for instance in [
            {"id": "xyzabc-def-ha", "hrid": "a123345"},
            {"id": "d97eeaae-9087-5b38-a78b-e789d0ab67f0", "hrid": "a700000"},
        ]:
            fo.write(f"{json.dumps(instance)}\n")

    holdings_filepath = results_dir / "folio_holdings_tsv-transformer.json"
    with holdings_filepath.open("w+") as fo:
        for holding in mock_holding_records:
            fo.write(f"{json.dumps(holding)}\n")

    electronic_holdings_filepath = (
        results_dir / "folio_holdings_electronic-transformer.json"
    )
    with electronic_holdings_filepath.open("w+") as fo:
        for holding in [
            {
                "instanceId": "xyzabc-def-ha",
                "id": "5d2bbdcd-0fe9-4725-8eaa-f0ba3b3b8b55",
                "permanentLocationId": "048eda24-a3af-4d2f-94f5-efe039d50c79",
            }
        ]:
            fo.write(f"{json.dumps(holding)}\n")

    generate_holdings_identifiers(airflow=airflow, dag_run=mock_dag_run)

    assert not holdings_filepath.exists()

    with (results_dir / "folio_holdings.json").open() as fo:
        modified_holdings = [json.loads(line) for line in fo.readlines()]

    assert modified_holdings[0]["hrid"] == "ah123345_1"
    assert modified_holdings[0]["id"] == "abcdedf123345"

    assert modified_holdings[1]["hrid"] == "ah123345_2"

    assert modified_holdings[2]["id"] == "5d2bbdcd-0fe9-4725-8eaa-f0ba3b3b8b55"
    assert modified_holdings[2]["hrid"] == "ah123345_3"


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = lambda *args, **kwargs: "ckey_0001.tsv"  # noqa


def test_generate_item_identifiers(
    mock_file_system, mock_dag_run, mock_okapi_variable, caplog  # noqa
):

    airflow = mock_file_system[0]
    iteration_dir = mock_file_system[2]
    results_dir = mock_file_system[3]

    mock_source_tsv = iteration_dir / "source_data/items/ckey_0001.tsv"

    with mock_source_tsv.open("w+") as fo:
        for row in [
            "BARCODE\tBASE_CALL_NUMBER",
            "36105080396299\tCSA 1.10:W 99/ ",
            "36105080396356\tPREX 2.8/3:962",
        ]:
            fo.write(f"{row}\n")

    mock_instances_holdings_items_path = results_dir / "instance-holdings-items.json"
    with mock_instances_holdings_items_path.open("w+") as fo:
        json.dump(instances_holdings_items_map, fo)

    mock_items_filepath = results_dir / "folio_items_transformer.json"
    with mock_items_filepath.open("w+") as fo:
        for item in [
            {
                "id": "f9262e00-1501-5f6e-a9ee-cc14a8f641ec",
                "holdingsRecordId": "abcdedf123345",
                "permanentLocationId": "b26d05ad-80a3-460d-8b49-00ddb72593bc",
                "barcode": "36105080396299",
            },
            {
                "id": "1cde0ebe-2cdd-5c9c-90f3-d4ce0dc63587",
                "holdingsRecordId": "65ae60f2-2340-4126-a690-242d19b598f7",
                "permanentLocationId": "b26d05ad-80a3-460d-8b49-00ddb72593bc",
            },
        ]:
            fo.write(f"{json.dumps(item)}\n")

    generate_item_identifiers(
        airflow=airflow, dag_run=mock_dag_run, task_instance=MockTaskInstance()
    )

    with mock_items_filepath.open() as fo:
        mock_modified_items = [json.loads(line) for line in fo.readlines()]

    assert mock_modified_items[0]["holdingsRecordId"] == "abcdedf123345"
    assert mock_modified_items[0]["hrid"] == "ai123345_1_1"

    assert (
        "65ae60f2-2340-4126-a690-242d19b598f7 not in found in Holdings" in caplog.text
    )

    with mock_instances_holdings_items_path.open() as fo:
        modified_instances_holdings_items_map = json.load(fo)

    assert modified_instances_holdings_items_map != instances_holdings_items_map
    assert (
        len(
            modified_instances_holdings_items_map["xyzabc-def-ha"]["holdings"][
                "abcdedf123345"
            ]["items"]
        )
        == 2
    )
