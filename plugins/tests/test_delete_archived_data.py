import os
from datetime import datetime, timedelta
from pathlib import Path

from dags.delete_archived_data import delete_archived_data
from plugins.folio.helpers import archive_artifacts

from plugins.tests.mocks import mock_dag_run, mock_file_system # noqa


def test_delete_archived_data(mock_dag_run, mock_file_system): # noqa
    dag = mock_dag_run
    airflow_path = mock_file_system[0]
    results_dir = mock_file_system[3]
    archive_dir = mock_file_system[4]

    # Create mock Instance JSON file
    instance_filename = f"folio_instances_bibs-transformer.json"
    instance_file = results_dir / instance_filename
    instance_file.write_text("""{ "id":"abcded2345"}""")

    yesterday = datetime.timestamp(datetime.today() - timedelta(days=1))
    os.utime(instance_file, times=(yesterday, yesterday))

    archive_artifacts(dag_run=dag, airflow=airflow_path)

    delete_archived_data(airflow=airflow_path, dag_run=dag)

    files = Path(archive_dir).glob("**/*")
    assert not (any(files))
