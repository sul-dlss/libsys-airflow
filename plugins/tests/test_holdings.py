import json

import pytest  # noqa
import pydantic

from plugins.folio.holdings import (
    electronic_holdings,
    post_folio_holding_records,
    merge_update_holdings,
    run_holdings_tranformer,
    run_mhld_holdings_transformer,
)

from plugins.tests.mocks import (  # noqa
    mock_okapi_success,
    mock_dag_run,
    mock_okapi_variable,
    mock_file_system,
    MockFOLIOClient,
)


class MockMapper(pydantic.BaseModel):
    folio_client: MockFOLIOClient = MockFOLIOClient()


class MockHoldingsConfiguration(pydantic.BaseModel):
    name = "holdings-transformer"


class MockFolderStructure(pydantic.BaseModel):
    data_issue_file_path: str = "results"


class MockHoldingsTransformer(pydantic.BaseModel):
    do_work = lambda x: "working"  # noqa
    mapper: MockMapper = MockMapper()
    folder_structure: MockFolderStructure = MockFolderStructure()
    folio_client: MockFOLIOClient = MockFOLIOClient()
    task_configuration: MockHoldingsConfiguration = MockHoldingsConfiguration()
    wrap_up = lambda x: "wrap_up"  # noqa


class MockTaskInstance(pydantic.BaseModel):
    xcom_pull = lambda *args, **kwargs: {}  # noqa
    xcom_push = lambda *args, **kwargs: None  # noqa


def test_electronic_holdings_missing_file(mock_dag_run, caplog):  # noqa
    electronic_holdings(
        dag_run=mock_dag_run,
        task_instance=MockTaskInstance(),
        library_config={},
        holdings_stem="holdings-transformers",
        holdings_type_id="1asdfasdfasfd",
        electronic_holdings_id="asdfadsfadsf",
    )
    assert (
        f"Electronic Holdings /opt/airflow/migration/iterations/{mock_dag_run.run_id}/source_data/items/holdings-transformers.electronic.tsv does not exist"
        in caplog.text
    )


def test_merge_update_holdings_no_holdings(
    mock_okapi_variable, mock_file_system, mock_dag_run, caplog  # noqa
):
    merge_update_holdings(airflow=str(mock_file_system[0]), dag_run=mock_dag_run)

    assert "No MHLDs holdings" in caplog.text


holdings = [
    {
        "id": "abcdedf123345",
        "hrid": "ah123345_1",
        "instanceId": "xyzabc-def-ha",
        "formerIds": ["a123345"],
        "permanentLocationId": "0edeef57-074a-4f07-aee2-9f09d55e65c3",
        "callNumber": "AB 12345",
    },
    {
        "id": "exyqdf123345",
        "hrid": "ah123345_2",
        "instanceId": "xyzabc-def-ha",
        "formerIds": ["a123345"],
        "permanentLocationId": "04c54d2f-0e14-42ab-97a6-27fc7f4d061",
    },
]

mhld_holdings = [
    {
        "id": "7e31c879-af1d-53fb-ba7a-60ad247a8dc4",
        "instanceId": "xyzabc-def-ha",
        "permanentLocationId": "0edeef57-074a-4f07-aee2-9f09d55e65c3",
        "holdingsStatements": [
            {
                "statement": "1914/1916-1916/1918,1932/1934-1934/1936",
                "note": "",
                "staffNote": "",
            }
        ],
        "holdingsStatementsForIndexes": [
            {"statement": "No indices exist", "note": "", "staffNote": ""}
        ],
    },
    {
        "id": "d1e33e3-3b57-53e4-bba0-b2faed059f40",
        "instanceId": "xyzabc-def-ha",
        "permanentLocationId": "782c40a2-51ba-4176-8b03-2abb96ee89b4",
        "holdingsStatementsForSupplements": [
            {"statement": "For years 2022-2023", "note": "", "staffNote": ""}
        ],
        "notes": [{"note": "a short note"}],
    },
]

srs_mhdls = [
    {
        "id": "",
        "externalIdsHolder": {
            "holdingsHrid": "ah1234566",
            "holdingsId": "7e31c879-af1d-53fb-ba7a-60ad247a8dc4",
        },
        "parsedRecord": {
            "id": "",
            "content": {
                "fields": [
                    {"001": "123344"},
                    {"004": "a1234566"},
                    {
                        "852": {
                            "subfields": [{"a": "CSt"}, {"b": "ART"}, {"c": "STACKS"}]
                        }
                    },
                    {
                        "999": {
                            "subfields": [
                                {"i": "3e5242ad-fec6-53c4-8dee-eaa807ab7f4d"},
                                {"s": "6e976de1-6c08-5159-b06b-af72d9a6bc26"},
                            ],
                            "ind1": "f",
                            "ind2": "f",
                        }
                    },
                ]
            },
        },
        "rawRecord": {"id": "", "content": {"fields": []}},
    },
    {
        "id": "",
        "externalIdsHolder": {
            "holdingsHrid": "ah13430268",
            "holdingsId": "d1e33e3-3b57-53e4-bba0-b2faed059f40",
        },
        "parsedRecord": {
            "id": "",
            "content": {"fields": [{"004": "a13430268"}]},
        },
        "rawRecord": {"id": "", "content": {"fields": []}},
    },
]


instances_holdings_map = {
    "xyzabc-def-ha": {
        "hrid": "a123345",
        "holdings": {
            "abcdedf123345": {
                "location_id": "0edeef57-074a-4f07-aee2-9f09d55e65c3",
                "merged": False,
            },
            "exyqdf123345": {
                "location_id": "04c54d2f-0e14-42ab-97a6-27fc7f4d061",
                "merged": False,
            },
            "nweoasdf42425": {  # Stand-in for Electronic Holding
                "location_id": "16211e24-f904-47f8-beaa-f91b4646c434",
                "merged": True,
            },
        },
    }
}


def test_merge_update_holdings(
    mock_okapi_variable, mock_file_system, mock_dag_run, caplog  # noqa
):
    results_dir = mock_file_system[3]
    holdings_tsv = results_dir / "folio_holdings_tsv-transformer.json"
    holdings_mhld = results_dir / "folio_holdings_mhld-transformer.json"

    with (holdings_tsv).open("w+") as fo:
        for row in holdings:
            fo.write(f"{json.dumps(row)}\n")

    with (holdings_mhld).open("w+") as fo:
        for row in mhld_holdings:
            fo.write(f"{json.dumps(row)}\n")

    with (results_dir / "instance_holdings_map.json").open("w+") as fo:
        json.dump(instances_holdings_map, fo)

    with (results_dir / "folio_srs_holdings_mhld-transformer.json").open("w+") as fo:
        for row in srs_mhdls:
            fo.write(f"{json.dumps(row)}\n")

    assert holdings_tsv.exists()
    assert holdings_mhld.exists()

    merge_update_holdings(airflow=str(mock_file_system[0]), dag_run=mock_dag_run)

    with (results_dir / "folio_holdings.json").open() as fo:
        combined_holdings = [json.loads(line) for line in fo.readlines()]

    assert not holdings_tsv.exists()
    assert not holdings_mhld.exists()

    # Tests merged Holdings with MHLD Holdings Record 1
    assert combined_holdings[0]["hrid"] == "ah123345_1"
    assert combined_holdings[0]["callNumber"] == "AB 12345"
    assert (
        combined_holdings[0]["holdingsStatements"][0]["statement"]
        == "1914/1916-1916/1918,1932/1934-1934/1936"
    )

    # Tests TSV Holding that didn't match
    assert combined_holdings[1]["hrid"] == "ah123345_2"
    assert "holdingsStatements" not in combined_holdings[1]

    # Test Added MHLD Holding that didn't match
    assert combined_holdings[2]["hrid"] == "ah123345_4"
    assert (
        combined_holdings[2]["holdingsStatementsForSupplements"][0]["statement"]
        == "For years 2022-2023"
    )

    # Tests modifications to the MHLDs MARC Record
    with (results_dir / "folio_srs_holdings_mhld-transformer.json").open() as fo:
        modified_srs = [json.loads(line) for line in fo.readlines()]
    first_rec_fields = modified_srs[0]["parsedRecord"]["content"]["fields"]
    assert first_rec_fields[0]["001"] == "ah123345_1"
    assert first_rec_fields[1]["004"] == "a1234566"
    assert (
        first_rec_fields[2]["852"]["subfields"][1]["b"]
        == "0edeef57-074a-4f07-aee2-9f09d55e65c3"
    )
    assert len(first_rec_fields[2]["852"]["subfields"]) == 2


def test_post_folio_holding_records(
    mock_okapi_success, mock_dag_run, mock_okapi_variable, tmp_path, caplog  # noqa
):

    dag = mock_dag_run

    holdings_json = tmp_path / f"holdings-{dag.run_id}-1.json"
    holdings_json.write_text(
        """[{ "id": "1233adf" },
    { "id": "45ryry" }]"""
    )

    post_folio_holding_records(
        tmp_dir=tmp_path, task_instance=MockTaskInstance(), dag_run=dag, job=1
    )

    assert "Result status code 201 for 2 records" in caplog.text


def test_run_holdings_tranformer():
    assert run_holdings_tranformer


def test_run_mhld_holdings_transformer(mock_file_system):  # noqa
    assert run_mhld_holdings_transformer
