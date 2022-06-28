import pytest  # noqa

from plugins.folio.marc import post_marc_to_srs, remove_srs_json

from plugins.tests.mocks import (
    mock_okapi_success,
    mock_dag_run,
    mock_okapi_variable,
    MockFOLIOClient,
    MockLibraryConfig,
    MockTaskInstance
)

@pytest.fixture
def srs_file(tmp_path):
    results_dir = tmp_path / "migration/results"
    results_dir.mkdir(parents=True)

    srs_filepath = results_dir / "test-srs.json"

    srs_filepath.write_text(
        """{ "id": "e9a161b7-3541-54d6-bd1d-e4f2c3a3db79",
      "rawRecord": {"content": "{\"leader\": \"01634pam a2200433 i 4500\", }}}"""
    )
    return srs_filepath

def test_remove_srs_json(srs_file, tmp_path):
    remove_srs_json(airflow=tmp_path, srs_filename="test-srs.json")

    assert srs_file.exists() is False


def test_post_marc_to_srs(srs_file, mock_okapi_success, mock_dag_run, mock_okapi_variable, tmp_path, caplog):
    dag = mock_dag_run

    library_config = MockLibraryConfig()

    post_marc_to_srs(
        dag_run=dag,
        library_config=library_config,
        srs_file="test-srs.json"
    )

    assert library_config.iteration_identifier == dag.run_id
    assert "Finished posting MARC json to SRS" in caplog
