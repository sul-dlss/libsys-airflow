import pytest

from plugins.folio.helpers import (
    archive_artifacts,
    move_marc_files_check_csv,
    process_marc,
    tranform_csv_to_tsv,
)


@pytest.fixture(scope="session")
def airflow_results(tmp_path_factory):
    dag_id = "manual__2022-02-18T18:36:12.440679+00:00"
    airflow_tmp_directory = tmp_path_factory.mkdir("opt") / "airflow/"
    migration_results_dir = airflow_tmp_directory / "migration/results/"
    test_instance = (
        migration_results_dir / f"folio_instances_{dag_id}_bibs-transformers.json"  # noqa
    )
    test_instance.write("""[{ "id": "12345-6789"}]""")
    test_holdings = (
        migration_results_dir / f"folio_holdings_{dag_id}_holdings-transformer.json"  # noqa
    )
    test_holdings.write("""[{ "id": "45678-9123"}]""")
    test_items = migration_results_dir / f"folio_items_{dag_id}_bibs-transformer.json"  # noqa
    test_items.write("""[{ "id": "67890-1234"}]""")
    return airflow_tmp_directory


def test_archive_artifacts(airflow_results):
    assert archive_artifacts


def test_move_marc_files():
    assert move_marc_files_check_csv


def test_process_marc():
    assert process_marc


def test_tranform_csv_to_tsv():
    assert tranform_csv_to_tsv
