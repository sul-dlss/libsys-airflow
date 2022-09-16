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
        "Electronic Holdings /opt/airflow/migration/data/items/holdings-transformers.electronic.tsv does not exist"
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


def _mock_setup_holdings_json(results_dir: pathlib.Path, dag_run_id: str):
    holdings_result_file = (
        results_dir / f"folio_holdings_{dag_run_id}_holdings-transformer.json"
    )

    with holdings_result_file.open("w+") as holdings_fo:
        for holding in holdings:
            holdings_fo.write(f"{json.dumps(holding)}\n")


def test_run_mhld_holdings_transformer(mock_file_system):  # noqa
    assert run_mhld_holdings_transformer


def test_add_identifiers_missing_hrid_file(mock_file_system):  # noqa
    transformer = MockHoldingsTransformer()
    dag_run_id = "manual_02022-09-16"
    results_dir = mock_file_system[3]

    _mock_setup_holdings_json(results_dir, dag_run_id)

    _add_identifiers(str(mock_file_system[0]), dag_run_id, transformer)

    holdings_result_file = (
        results_dir / f"folio_holdings_{dag_run_id}_holdings-transformer.json"
    )
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


def test_add_identifiers_existing_hrid(mock_file_system):  # noqa
    transformer = MockHoldingsTransformer()
    dag_run_id = "manual_02022-09-16T22:49:19"
    results_dir = mock_file_system[3]

    _mock_setup_holdings_json(results_dir, dag_run_id)

    # Mocks holdings_hrid file
    holdings_hrid_file = results_dir / f"holdings-hrids-{dag_run_id}.json"

    with holdings_hrid_file.open("w+") as fo:
        fo.write(json.dumps({"xyzabc-def-ha": 2}))

    _add_identifiers(str(mock_file_system[0]), dag_run_id, transformer)

    holdings_result_file = (
        results_dir / f"folio_holdings_{dag_run_id}_holdings-transformer.json"
    )
    with holdings_result_file.open() as holdings_fo:
        holdings_records = [json.loads(line) for line in holdings_fo.readlines()]

    # Test HRIDs
    assert holdings_records[0]["hrid"] == "ah123345_3"


def test_run_transformer(mock_file_system, caplog):  # noqa
    dag_run_id = "manual_02022-09-17T00:49:19"
    results_dir = mock_file_system[3]

    _mock_setup_holdings_json(results_dir, dag_run_id)

    _run_transformer(MockHoldingsTransformer(), str(mock_file_system[0]), dag_run_id)

    assert "Adding HRIDs and re-generated UUIDs for holdings" in caplog.text

    # Removes file artifact created by setup_data_logging's data_issue_file_handler
    empty_results = pathlib.Path("results")
    if empty_results.exists():
        empty_results.unlink()


def test_consolidate_holdings_map(mock_file_system, mock_dag_run, caplog):  # noqa
    results_dir = mock_file_system[3]

    holdings_id_map = results_dir / f"holdings_id_map_{mock_dag_run.run_id}.json"
    holdings_id_map.touch()

    holdings_id_map_all = (
        results_dir / f"holdings_id_map_all_{mock_dag_run.run_id}.json"
    )
    holdings_id_map_all.write_text(
        json.dumps({"legacy_code": "abcded", "folio_id": "efcageh"})
    )

    consolidate_holdings_map(airflow=str(mock_file_system[0]), dag_run=mock_dag_run)

    assert f"Finished moving {holdings_id_map_all} to {holdings_id_map}" in caplog.text
