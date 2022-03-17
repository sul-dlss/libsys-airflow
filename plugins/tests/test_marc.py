import json

from plugins.folio.marc import replace_srs_record_type
from plugins.tests.test_helpers import mock_dag_run, mock_file_system  # noqa


def test_replace_srs_record_type(mock_dag_run, mock_file_system):  # noqa
    dag = mock_dag_run
    airflow_path = mock_file_system[0]
    results_path = mock_file_system[3]

    # Mock SRS Record
    srs_mock_filename = (
        f"folio_srs_instances_{dag.run_id}_bibs-transformer.json"  # noqa
    )
    srs_mock_file = results_path / srs_mock_filename
    srs_mock_file.write_text("""{ "recordType": "MARC_BIB" }""")

    replace_srs_record_type(airflow=airflow_path, dag_run=dag)

    srs_json_doc = json.loads(srs_mock_file.read_text())

    assert srs_json_doc["recordType"] == "MARC"
