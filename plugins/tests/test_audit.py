import pathlib

import pytest

from plugins.tests.mocks import mock_dag_run, mock_file_system  # noqa
from plugins.folio.audit import audit_instance_views, setup_audit_db


def test_setup_audit_db(mock_dag_run, mock_file_system):  # noqa
    airflow = mock_file_system[0]

    iterations_dir = mock_file_system[2]

    current_file = pathlib.Path(__file__)
    db_init_file = current_file.parents[2] / "migration/qa.sql"
    mock_db_init_file = airflow / "migration/qa.sql"
    mock_db_init_file.write_text(db_init_file.read_text())

    audit_db_filepath = iterations_dir / "results/audit-remediation.db"

    assert audit_db_filepath.exists() is False

    setup_audit_db(airflow=airflow, iteration_id=mock_dag_run.run_id)

    assert audit_db_filepath.exists()