import json
import pathlib

import pytest  # noqa
import pydantic

from plugins.folio.holdings import (
    _add_identifiers,
    consolidate_holdings_map,
    electronic_holdings,
    post_folio_holding_records,
    run_holdings_tranformer,
    run_mhld_holdings_transformer,
    _run_transformer,
    update_mhlds_uuids,
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


holdings = [
    {
        "id": "abcdedf123345",
        "instanceId": "xyzabc-def-ha",
        "formerIds": ["a123345"],
        "callNumber": "A1234",
    },
    {
        "id": "exyqdf123345",
        "instanceId": "xyzabc-def-ha",
        "formerIds": ["a123345"],
        "callNumber": "B1234",
    },
]


def _mock_setup_holdings_json(iteration_dir: pathlib.Path):
    holdings_result_file = (
        iteration_dir / "results/folio_holdings_holdings-transformer.json"
    )

    with holdings_result_file.open("w+") as holdings_fo:
        for holding in holdings:
            holdings_fo.write(f"{json.dumps(holding)}\n")


def test_run_mhld_holdings_transformer(mock_file_system):  # noqa
    assert run_mhld_holdings_transformer


def test_add_identifiers_missing_hrid_file(mock_file_system, mock_dag_run):  # noqa
    transformer = MockHoldingsTransformer()

    iteration_dir = mock_file_system[2]
    results_dir = mock_file_system[3]

    _mock_setup_holdings_json(iteration_dir)

    _add_identifiers(str(mock_file_system[0]), mock_dag_run.run_id, transformer, None)

    holdings_result_file = results_dir / "folio_holdings_holdings-transformer.json"
    with holdings_result_file.open() as holdings_fo:
        holdings_records = [json.loads(line) for line in holdings_fo.readlines()]

    # Test UUIDS
    assert holdings_records[0]["id"] == "3000ae83-e7ee-5e3c-ab0c-7a931a23a393"
    assert holdings_records[1]["id"] == "67360f4a-fb55-5c78-ad11-585e1a6c6aa4"

    # Test HRIDs
    assert holdings_records[0]["hrid"] == "ah123345_1"
    assert holdings_records[1]["hrid"] == "ah123345_2"

    # Test _version
    assert holdings_records[0]["_version"] == 1


def test_add_identifiers_existing_hrid(mock_file_system, mock_dag_run):  # noqa
    transformer = MockHoldingsTransformer()
    dag_run_id = mock_dag_run.run_id
    iteration_dir = mock_file_system[2]
    results_dir = mock_file_system[3]

    _mock_setup_holdings_json(iteration_dir)

    # Mocks holdings_hrid file
    holdings_hrid_file = iteration_dir / "results/holdings-hrids.json"

    with holdings_hrid_file.open("w+") as fo:
        fo.write(json.dumps({"xyzabc-def-ha": 2}))

    _add_identifiers(str(mock_file_system[0]), dag_run_id, transformer, None)

    holdings_result_file = results_dir / "folio_holdings_holdings-transformer.json"
    with holdings_result_file.open() as holdings_fo:
        holdings_records = [json.loads(line) for line in holdings_fo.readlines()]

    # Test HRIDs
    assert holdings_records[0]["hrid"] == "ah123345_3"


def test_run_transformer(mock_file_system, mock_dag_run, caplog):  # noqa
    iteration_dir = mock_file_system[2]

    _mock_setup_holdings_json(iteration_dir)

    _run_transformer(
        MockHoldingsTransformer(), str(mock_file_system[0]), mock_dag_run.run_id, None
    )

    assert "Adding HRIDs and re-generated UUIDs for holdings" in caplog.text

    # Removes file artifact created by setup_data_logging's data_issue_file_handler
    empty_results = pathlib.Path("results")
    if empty_results.exists():
        empty_results.unlink()


def test_consolidate_holdings_map(mock_file_system, mock_dag_run, caplog):  # noqa
    results_dir = mock_file_system[3]

    holdings_id_map = results_dir / "holdings_id_map.json"
    holdings_id_map.touch()

    holdings_id_map_all = results_dir / "holdings_id_map_all.json"
    holdings_id_map_all.write_text(
        json.dumps({"legacy_code": "abcded", "folio_id": "efcageh"})
    )

    consolidate_holdings_map(airflow=str(mock_file_system[0]), dag_run=mock_dag_run)

    assert f"Finished moving {holdings_id_map_all} to {holdings_id_map}" in caplog.text


def test_update_mhlds_uuids_no_srs(mock_dag_run, caplog):  # noqa
    update_mhlds_uuids(dag_run=mock_dag_run)

    assert "No MHLD SRS records" in caplog.text


def test_update_mhlds_uuids(
    mock_file_system, mock_dag_run, mock_okapi_variable, caplog  # noqa  # noqa  # noqa
):  # noqa
    results_dir = mock_file_system[3]

    mhld_srs_mock_file = results_dir / "folio_srs_holdings_mhld-transformer.json"

    with mhld_srs_mock_file.open("w+") as fo:
        for srs_rec in [
            {
                "id": "",
                "externalIdsHolder": {
                    "holdingsHrid": "ah1234566",
                    "holdingsId": "7e31c879-af1d-53fb-ba7a-60ad247a8dc4",
                },
                "parsedRecord": {
                    "id": "",
                    "content": {"fields": [{"004": "a1234566"}]},
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
        ]:
            fo.write(f"{json.dumps(srs_rec)}\n")

    holdings_id_map_mock = results_dir / "holdings_id_map.json"

    with holdings_id_map_mock.open("w+") as fo:
        for row in [
            {
                "legacy_id": "ah1234566",
                "folio_id": "1a3123ba-5dc4-4653-a2ae-5a972a3ad01f",
            }
        ]:
            fo.write(f"{json.dumps(row)}\n")

    holdings_records_mock = results_dir / "folio_holdings_mhld-transformer.json"

    with holdings_records_mock.open("w+") as fo:
        for row in [
            {
                "id": "1a3123ba-5dc4-4653-a2ae-5a972a3ad01f",
                "hrid": "ah1234566",
                "formerIds": ["a1234566"],
            }
        ]:
            fo.write(f"{json.dumps(row)}\n")

    update_mhlds_uuids(
        dag_run=mock_dag_run,
        airflow=mock_file_system[0],
    )

    with mhld_srs_mock_file.open() as fo:
        srs_records = [json.loads(line) for line in fo.readlines()]

    # Tests for output SRS records that have updated UUID
    assert len(srs_records) == 1
    assert (
        srs_records[0]["externalIdsHolder"]["holdingsId"]
        == "1a3123ba-5dc4-4653-a2ae-5a972a3ad01f"
    )

    # Tests for HRID not present in the mapping file
    assert "UUID for MHLD a13430268 not found in SRS record" in caplog.text
