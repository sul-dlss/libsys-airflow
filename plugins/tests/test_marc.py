import pytest  # noqa

import folio_migration_tools.migration_tasks.batch_poster as batch_poster

from plugins.folio.marc import post_marc_to_srs, remove_srs_json

from plugins.tests.mocks import (  # noqa
    mock_okapi_success,
    mock_dag_run,
    mock_file_system,
    mock_okapi_variable,
    MockFOLIOClient,
    MockLibraryConfig,
    MockTaskInstance,
)


@pytest.fixture
def mock_get_req_size(monkeypatch):
    def mock_size(response):
        return "150.00MB"

    monkeypatch.setattr(batch_poster, "get_req_size", mock_size)


@pytest.fixture
def srs_file(mock_file_system):  # noqa
    results_dir = mock_file_system[3]

    srs_filepath = results_dir / "test-srs.json"

    srs_filepath.write_text(
        """{ "id": "e9a161b7-3541-54d6-bd1d-e4f2c3a3db79", "rawRecord": { "content": {"leader": "01634pam a2200433 i 4500"}}}"""
    )
    return srs_filepath


def test_post_marc_to_srs(
    srs_file,
    mock_okapi_success,  # noqa
    mock_dag_run,  # noqa
    mock_file_system,  # noqa
    mock_get_req_size,
    mock_okapi_variable,  # noqa
    caplog,
):
    dag = mock_dag_run

    base_folder = mock_file_system[0] / "migration"

    library_config = MockLibraryConfig(base_folder=str(base_folder))

    post_marc_to_srs(
        dag_run=dag, library_config=library_config, srs_file="test-srs.json"
    )

    assert library_config.iteration_identifier == dag.run_id
    assert "Finished posting MARC json to SRS" in caplog.text


def test_remove_srs_json(srs_file, mock_file_system):  # noqa
    assert srs_file.exists() is True

    remove_srs_json(airflow=mock_file_system[0], srs_filename="test-srs.json")

    assert srs_file.exists() is False
