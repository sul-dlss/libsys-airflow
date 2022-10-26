import json

import pydantic

from plugins.folio.helpers.folio_ids import (
    generate_holdings_identifiers,
    generate_item_identifiers,
    _lookup_holdings_uuid,
)

from plugins.tests.test_holdings import holdings as mock_holding_records

from plugins.tests.mocks import mock_dag_run, mock_file_system, mock_okapi_variable  # noqa


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

    mhlds_holdings_filepath = results_dir / "folio_holdings_mhld-transformer.json"
    with mhlds_holdings_filepath.open("w+") as fo:
        for holding in [{"instanceId": "d97eeaae-9087-5b38-a78b-e789d0ab67f0"}]:
            fo.write(f"{json.dumps(holding)}\n")

    electronic_holdings_filepath = (
        results_dir / "folio_holdings_electronic-transformer.json"
    )
    with electronic_holdings_filepath.open("w+") as fo:
        for holding in [{"instanceId": "xyzabc-def-ha"}]:
            fo.write(f"{json.dumps(holding)}\n")

    generate_holdings_identifiers(airflow=airflow, dag_run=mock_dag_run)

    with holdings_filepath.open() as fo:
        modified_holdings = [json.loads(line) for line in fo.readlines()]

    assert modified_holdings[0]["hrid"] == "ah123345_1"
    assert modified_holdings[0]["id"] == "03e6d8da-9c1e-58d1-8459-4f657200a5df"

    assert modified_holdings[1]["hrid"] == "ah123345_2"

    with mhlds_holdings_filepath.open() as fo:
        mhlds_modified_holdings = [json.loads(line) for line in fo.readlines()]

    assert mhlds_modified_holdings[0]["hrid"] == "ah700000_1"

    with electronic_holdings_filepath.open() as fo:
        electronic_modified_holdings = [json.loads(line) for line in fo.readlines()]

    assert electronic_modified_holdings[0]["hrid"] == "ah123345_3"


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = lambda *args, **kwargs: "ckey_0001.tsv"  # noqa


def test_generate_item_identifiers(
    mock_file_system, mock_dag_run, mock_okapi_variable  # noqa
):

    airflow = mock_file_system[0]
    results_dir = mock_file_system[3]

    mock_holdings_items_map = results_dir / "holdings-items-map.json"
    with mock_holdings_items_map.open("w+") as fo:
        fo.write(
            json.dumps(
                {
                    "e9ff785b-97e1-5f00-8dd0-4fce8fef1da3": {
                        "1df5a25b-2b80-59a3-824a-1ab8f983cfaf": {
                            "hrid": "ah650005_1",
                            "items": [],
                            "permanentLocationId": "b26d05ad-80a3-460d-8b49-00ddb72593bc",
                        },
                    }
                }
            )
        )

    mock_items_filepath = results_dir / "folio_items_transformer.json"
    with mock_items_filepath.open("w+") as fo:
        for item in [
            {
                "id": "f9262e00-1501-5f6e-a9ee-cc14a8f641ec",
                "holdingsRecordId": "e9ff785b-97e1-5f00-8dd0-4fce8fef1da3",
                "permanentLocationId": "b26d05ad-80a3-460d-8b49-00ddb72593bc",
            },
            {
                "id": "1cde0ebe-2cdd-5c9c-90f3-d4ce0dc63587",
                "holdingsRecordId": "e9ff785b-97e1-5f00-8dd0-4fce8fef1da3",
                "permanentLocationId": "b26d05ad-80a3-460d-8b49-00ddb72593bc",
            },
        ]:
            fo.write(f"{json.dumps(item)}\n")

    generate_item_identifiers(
        airflow=airflow, dag_run=mock_dag_run, task_instance=MockTaskInstance()
    )

    with mock_items_filepath.open() as fo:
        mock_modified_items = [json.loads(line) for line in fo.readlines()]

    assert (
        mock_modified_items[0]["holdingsRecordId"]
        == "1df5a25b-2b80-59a3-824a-1ab8f983cfaf"
    )
    assert mock_modified_items[0]["hrid"] == "ai650005_1_1"

    assert (
        mock_modified_items[1]["holdingsRecordId"]
        == "1df5a25b-2b80-59a3-824a-1ab8f983cfaf"
    )
    assert mock_modified_items[1]["hrid"] == "ai650005_1_2"


def test_lookup_holdings_uuid():
    item_uuid_location_map = {
        "ad9fce9d-e538-5916-ba74-4bfbce9af81a": "0edeef57-074a-4f07-aee2-9f09d55e65c3",
        "994c8f9a-d622-566e-9a03-42703b6083d5": "2b94c631-fca9-4892-a730-03ee529ffe27",
        "36778651d-c393-5718-943e-dea3ebd73706": "2b94c631-fca9-4892-a730-03ee529ffe27",
    }

    holdings_map = {
        "527b783d-6624-56fe-be28-895f2c69cf1f": {
            "hrid": "ah660592_1",
            "permanentLocationId": "0edeef57-074a-4f07-aee2-9f09d55e65c3",
            "items": [],
        }
    }

    holdings_uuid = _lookup_holdings_uuid(
        item_uuid_location_map, holdings_map, "ad9fce9d-e538-5916-ba74-4bfbce9af81a"
    )
    assert holdings_uuid == "527b783d-6624-56fe-be28-895f2c69cf1f"
