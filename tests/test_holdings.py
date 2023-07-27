import json

from copy import deepcopy

import pytest  # noqa
import pydantic
import pymarc

from libsys_airflow.plugins.folio.holdings import (
    electronic_holdings,
    holdings_only_notes,
    post_folio_holding_records,
    update_holdings,
    run_holdings_tranformer,
    run_mhld_holdings_transformer,
    boundwith_holdings,
    _add_holdings_type_ids,
    _alt_condition_remove_ending_punc,
    _alt_get_legacy_ids,
    _wrap_additional_mapping,
)

from tests.mocks import (  # noqa
    mock_okapi_success,
    mock_dag_run,
    mock_okapi_variable,
    mock_file_system,
    MockFOLIOClient,
    MockTaskInstance,
)

import tests.mocks as mocks


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


def test_wrap_additional_mapping():
    @_wrap_additional_mapping
    def additional_mapping(placeholder, holding):
        assert holding["permanentLocationId"] == "baae3735-97c5-486d-9c6b-c87c41ae51ab"
        assert holding["callNumberTypeId"] == "95467209-6d7b-468b-94df-0f5d7ad2747d"
        assert holding["copyNumber"] == "1"
        assert "permanentLocationId" not in holding["holdingsStatements"]
        assert len(holding["holdingsStatements"]) == 1
        assert holding["callNumber"] == "MARC Holdings"

    incorrect_holding = {
        "holdingsStatements": [
            {
                "callNumberTypeId": "95467209-6d7b-468b-94df-0f5d7ad2747d",
                "permanentLocationId": "baae3735-97c5-486d-9c6b-c87c41ae51ab",
                "copyNumber": "1",
            },
            {"statement": "1988;1994;1999"},
        ]
    }
    additional_mapping(None, incorrect_holding)


holdings = [
    {
        "id": "abcdedf123345",
        "hrid": "ah123345_1",
        "instanceId": "xyzabc-def-ha",
        "formerIds": ["a123345"],
        "permanentLocationId": "0edeef57-074a-4f07-aee2-9f09d55e65c3",
        "callNumber": "AB 12345",
        "sourceId": "f32d531e-df79-46b3-8932-cdd35f7a2264",
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
        "sourceId": "036ee84a-6afd-4c3c-9ad3-4a12ab875f59",
    },
    {
        "id": "d1e33e3-3b57-53e4-bba0-b2faed059f40",
        "instanceId": "xyzabc-def-ha",
        "permanentLocationId": "782c40a2-51ba-4176-8b03-2abb96ee89b4",
        "holdingsStatementsForSupplements": [
            {"statement": "For years 2022-2023", "note": "", "staffNote": ""}
        ],
        "notes": [{"note": "a short note"}],
        "sourceId": "036ee84a-6afd-4c3c-9ad3-4a12ab875f59",
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


instances_holdings_items_map = {
    "xyzabc-def-ha": {
        "hrid": "a123345",
        "holdings": {
            "abcdedf123345": {
                "hrid": "ah123345_1",
                "permanentLocationId": "0edeef57-074a-4f07-aee2-9f09d55e65c3",
                "merged": False,
                "items": [],
            },
            "exyqdf123345": {
                "hrid": "ah123345_2",
                "permanentLocationId": "21b7083b-1013-440e-8e62-64169824dcb8",
                "merged": False,
                "items": [],
            },
            "nweoasdf42425": {  # Stand-in for Electronic Holding
                "hrid": "ah123345_3",
                "permanentLocationId": "b0a1a8c3-cc9a-487c-a2ed-308fc3a49a91",
                "merged": False,
                "items": [],
            },
        },
    }
}


def test_update_holdings(
    mock_okapi_variable, mock_file_system, mock_dag_run, caplog  # noqa
):
    results_dir = mock_file_system[3]
    holdings_tsv = results_dir / "folio_holdings.json"
    holdings_mhld = results_dir / "folio_holdings_mhld-transformer.json"

    with (holdings_tsv).open("w+") as fo:
        for row in holdings:
            fo.write(f"{json.dumps(row)}\n")

    with (holdings_mhld).open("w+") as fo:
        for row in mhld_holdings:
            fo.write(f"{json.dumps(row)}\n")

    with (results_dir / "instance-holdings-items.json").open("w+") as fo:
        json.dump(instances_holdings_items_map, fo)

    with (results_dir / "folio_srs_holdings_mhld-transformer.json").open("w+") as fo:
        for row in srs_mhdls:
            fo.write(f"{json.dumps(row)}\n")

    assert holdings_tsv.exists()
    assert holdings_mhld.exists()

    mock_folio_client = MockFOLIOClient(
        locations=[{"id": "0edeef57-074a-4f07-aee2-9f09d55e65c3", "code": "GRE-STACKS"}]
    )

    update_holdings(
        airflow=str(mock_file_system[0]),
        dag_run=mock_dag_run,
        folio_client=mock_folio_client,
    )

    with (results_dir / "folio_holdings.json").open() as fo:
        combined_holdings = [json.loads(line) for line in fo.readlines()]

    assert not holdings_mhld.exists()

    # Tests TSV Holding that didn't match
    assert combined_holdings[1]["hrid"] == "ah123345_2"
    assert "holdingsStatements" not in combined_holdings[1]

    # Test MHLD Holding
    assert combined_holdings[3]["hrid"] == "ah123345_5"
    assert (
        combined_holdings[3]["holdingsStatementsForSupplements"][0]["statement"]
        == "For years 2022-2023"
    )

    # Tests modifications to the MHLDs MARC Record
    with (results_dir / "folio_srs_holdings_mhld-transformer.json").open() as fo:
        modified_srs = [json.loads(line) for line in fo.readlines()]
    first_rec_fields = modified_srs[0]["parsedRecord"]["content"]["fields"]
    assert first_rec_fields[0]["001"] == "ah123345_4"
    assert first_rec_fields[1]["004"] == "a1234566"
    assert first_rec_fields[2]["852"]["subfields"][1]["b"] == "GRE-STACKS"
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
    # if the import is successful, this will always pass because the function ref is truthy, hence the typechecker complaint
    assert run_holdings_tranformer  # type: ignore


def test_run_mhld_holdings_transformer(mock_file_system):  # noqa
    # if the import is successful, this will always pass because the function ref is truthy, hence the typechecker complaint
    assert run_mhld_holdings_transformer  # type: ignore


def _mock_folio_get(url: str) -> dict:
    output = {}
    if url.startswith("/holdings-note-types"):
        output["holdingsNoteTypes"] = [
            {
                "id": "1c83e151-d1ef-4144-a52d-cc9762aa4923",
                "name": "Tech Staff",
            },
        ]
    if url.startswith("/holdings-types"):
        output["holdingsTypes"] = [
            {
                "id": "5684e4a3-9279-4463-b6ee-20ae21bbec07",
                "name": "Book",
            },
            {
                "id": "5b08b35d-aaa3-4806-998c-9cd85e5bc406",
                "name": "Bound-with",
            },
            {
                "id": "996f93e2-5b5e-4cf2-9168-33ced1f95eed",
                "name": "Electronic",
            },
            {"id": "f6ba0bff-5674-445b-9922-8451d0365814", "name": "Unknown"},
        ]
    return output


def test_boundwith_holdings(
    mock_dag_run, mock_okapi_variable, mock_file_system  # noqa
):
    dag = mock_dag_run

    bw_tsv = mock_file_system[1] / "ckeys_.tsv.bwchild.tsv"
    mocks.messages["bib-files-group"] = {"bwchild-file": str(bw_tsv)}

    bw_tsv_lines = [
        "CATKEY\tCALL_SEQ\tCOPY\tBARCODE\tLIBRARY\tHOMELOCATION\tCURRENTLOCATION\tITEM_TYPE\tITEM_CAT1\tITEM_CAT2\tITEM_SHADOW\tCALL_NUMBER_TYPE\tBASE_CALL_NUMBER\tVOLUME_INFO\tCALL_SHADOW\tFORMAT\tCATALOG_SHADOW",
        "2956972\t2\t1\t36105127895816\tGREEN\tSEE-OTHER\tSEE-OTHER\tGOVSTKS\tBW-CHILD\t\t0\tSUDOC\tI\t29.9/5:148\t\t0\tMARC\t0",
        "2950696\t2\t1\t\tSAL3\tSEE-OTHER\tSEE-OTHER\tSTKS-MONO\tBW-CHILD\t\t0\t1\tALPHANUM\t353 PAM\tC:NO.6\t0\tMARC\t0",
    ]

    with bw_tsv.open("w+") as fo:
        for line in bw_tsv_lines:
            fo.write(f"{line}\n")

    holdingsnotes_bw_tsv = mock_file_system[1] / "ckeys_.holdingsnote.tsv"
    mocks.messages["bib-files-group"]["tsv-holdingsnotes"] = str(holdingsnotes_bw_tsv)

    with holdingsnotes_bw_tsv.open("w+") as fo:
        for line in [
            "CATKEY\tCALL_SEQ\tCOPY\tLIBRARY\tHOMELOCATION\tCURRENTLOCATION\tITEM_TYPE\tITEM_CAT1\tNOTE_TYPE\tNOTE",
            "2956972\t2\t1\tGREEN\tFED-DOCS\tFED-DOCS\tNONCIRC\tBW-CHILD\tTECHSTAFF\tac:yd, 4/20/23",
        ]:
            fo.write(f"{line}\n")

    holdings_json = mock_file_system[3] / "folio_holdings_boundwith.json"

    mock_folio_client = MockFOLIOClient(
        locations=[
            {"id": "0edeef57-074a-4f07-aee2-9f09d55e65c3", "code": "GRE-SEE-OTHER"},
            {"id": "58168a3-ede4-4cc1-8c98-61f4feeb22ea", "code": "SAL3-SEE-OTHER"},
        ]
    )

    mock_folio_client.folio_get = _mock_folio_get

    boundwith_holdings(
        airflow=mock_file_system[0],
        dag_run=dag,
        folio_client=mock_folio_client,
        task_instance=MockTaskInstance(),
    )

    with holdings_json.open() as hld:
        holdings_records = [json.loads(line) for line in hld.readlines()]

    assert len(holdings_records) == 2
    assert holdings_records[1]["callNumber"] == "ALPHANUM 353 PAM"

    bw_part = mock_file_system[3] / "boundwith_parts.json"
    with bw_part.open() as bwp:
        bw_part_rec = [json.loads(line) for line in bwp.readlines()]

    assert len(bw_part_rec) == 1
    assert holdings_records[0]["id"] == bw_part_rec[0]["holdingsRecordId"]
    assert holdings_records[0]["notes"][0]["note"] == "ac:yd, 4/20/23"


def test_add_holdings_type_ids(mock_file_system, mock_dag_run):  # noqa
    mock_folio_client = MockFOLIOClient()
    mock_folio_client.folio_get = _mock_folio_get

    holdings_path = mock_file_system[3] / "folio_holdings_tsv-transformer.json"

    holdings = [
        {"id": "3e36dd80-057e-5a84-8c7a-a35737c944fb", "holdingsTypeId": "MARC"},
        {"id": "56840dc5-88fd-5481-866d-b028e4acf782", "holdingsTypeId": "MAGAZINE"},
    ]

    with holdings_path.open("w+") as fo:
        for row in holdings:
            fo.write(f"{json.dumps(row)}\n")

    _add_holdings_type_ids(
        airflow=mock_file_system[0],
        dag_run_id=mock_dag_run.run_id,
        folio_client=mock_folio_client,
    )

    with holdings_path.open() as fo:
        mod_holdings = [json.loads(line) for line in fo.readlines()]

    assert mod_holdings[0]["holdingsTypeId"] == "5684e4a3-9279-4463-b6ee-20ae21bbec07"
    assert mod_holdings[1]["holdingsTypeId"] == "f6ba0bff-5674-445b-9922-8451d0365814"


def test_alt_condition_remove_ending_punc():
    open_end_periodical_range = _alt_condition_remove_ending_punc(
        None, None, "v.27(2014),v.32(2016)-"
    )
    assert open_end_periodical_range == "v.27(2014),v.32(2016)-"
    removed_end_punc = _alt_condition_remove_ending_punc(None, None, "A note=")
    assert removed_end_punc == "A note"


def test_alt_get_legacy_ids():
    marc_record = pymarc.Record()
    field_001 = pymarc.Field(tag="001", data="1964746")
    marc_record.add_field(field_001)
    field_852 = pymarc.Field(
        tag="852",
        subfields=[
            pymarc.Subfield(code="b", value="SAL3"),
            pymarc.Subfield(code="c", value="PAGE-GR"),
        ],
    )
    marc_record.add_field(field_852)
    legacy_id = _alt_get_legacy_ids(None, None, marc_record)
    assert legacy_id == ["1964746 SAL3 PAGE-GR"]


def test_holdings_only_notes(mock_file_system, mock_dag_run):  # noqa
    results_dir = mock_file_system[3]
    holdings_json = results_dir / "folio_holdings.json"
    holdingsnotes_tsv = mock_file_system[1] / "ckeys_.holdingsnote.tsv"

    new_holdings = deepcopy(holdings)

    new_holdings.append(
        {
            "id": "22f7534d-1808-5796-bfe5-c2071cda9bc9",
            "holdingsTypeId": "996f93e2-5b5e-4cf2-9168-33ced1f95eed",
            "hrid": "ah14717849_1",
        }
    )

    # Sets holdingsTypeIds
    new_holdings[0]["holdingsTypeId"] = "5684e4a3-9279-4463-b6ee-20ae21bbec07"
    new_holdings[1]["holdingsTypeId"] = "5684e4a3-9279-4463-b6ee-20ae21bbec07"

    with holdings_json.open("w+") as fo:
        for holding in new_holdings:
            fo.write(f"{json.dumps(holding)}\n")

    mock_folio_client = MockFOLIOClient()
    mock_folio_client.folio_get = _mock_folio_get

    with holdingsnotes_tsv.open("w+") as fo:
        for line in [
            "CATKEY\tCALL_SEQ\tCOPY\tLIBRARY\tHOMELOCATION\tCURRENTLOCATION\tITEM_TYPE\tITEM_CAT1\tNOTE_TYPE\tNOTE",
            "14717849\t1\t1\tSUL\tINTERNET\tINTERNET\tSUL\t\tTECHSTAFF\taRetain 856 even if there is a 956. (krt,6/16/2023)",
            "123345\t2\t1\tGREEN\tBASECALNUM\tBASECALNUM\tSTKS-PERI\t\tTECHSTAFF\taBASE CALL NUMBER. DO NOT REMOVE OR USE.",
            "123345\t1\t1\tGREEN\tBASECALNUM\tBASECALNUM\tSTKS-PERI\t\tTECHSTAFF\tai:yd, 5/10/23",
        ]:
            fo.write(f"{line}\n")

    holdings_only_notes(
        airflow=mock_file_system[0],
        dag_run=mock_dag_run,
        holdingsnotes_tsv=str(holdingsnotes_tsv),
        folio_client=mock_folio_client,
    )

    with holdings_json.open() as fo:
        mod_holdings = [json.loads(line) for line in fo.readlines()]

    assert (
        mod_holdings[0]["notes"][0]["note"]
        == "aBASE CALL NUMBER. DO NOT REMOVE OR USE."
    )
    assert mod_holdings[0]["notes"][1]["note"] == "ai:yd, 5/10/23"
    assert "notes" not in mod_holdings[1]
    assert (
        mod_holdings[2]["notes"][0]["note"]
        == "aRetain 856 even if there is a 956. (krt,6/16/2023)"
    )
