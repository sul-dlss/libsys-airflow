import json

import pydantic

from plugins.folio.helpers.folio_ids import (
    generate_holdings_identifiers,
    generate_item_identifiers,
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

    electronic_holdings_filepath = (
        results_dir / "folio_holdings_electronic-transformer.json"
    )
    with electronic_holdings_filepath.open("w+") as fo:
        for holding in [
            {
                "instanceId": "xyzabc-def-ha",
                "permanentLocationId": "048eda24-a3af-4d2f-94f5-efe039d50c79",
            }
        ]:
            fo.write(f"{json.dumps(holding)}\n")

    generate_holdings_identifiers(airflow=airflow, dag_run=mock_dag_run)

    with holdings_filepath.open() as fo:
        modified_holdings = [json.loads(line) for line in fo.readlines()]

    assert modified_holdings[0]["hrid"] == "ah123345_1"
    assert modified_holdings[0]["id"] == "03e6d8da-9c1e-58d1-8459-4f657200a5df"

    assert modified_holdings[1]["hrid"] == "ah123345_2"

    with electronic_holdings_filepath.open() as fo:
        electronic_modified_holdings = [json.loads(line) for line in fo.readlines()]

    assert electronic_modified_holdings[0]["hrid"] == "ah123345_3"


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

    mock_holdings_items_map = results_dir / "holdings-items-map.json"
    with mock_holdings_items_map.open("w+") as fo:
        fo.write(
            json.dumps(
                {
                    "e9ff785b-97e1-5f00-8dd0-4fce8fef1da3": {
                        "1df5a25b-2b80-59a3-824a-1ab8f983cfaf": {
                            "hrid": "ah650005_1",
                            "callNumber": "CSA 1.10:W 99/",
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
                "barcode": "36105080396299",
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
        "New holdings UUID not found for item with permanentLocationId" in caplog.text
    )
    assert "Unable to retrieve generated holdings UUID" in caplog.text
