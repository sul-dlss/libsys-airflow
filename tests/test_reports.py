import pytest  # noqa
import sqlite3

from folio_uuid.folio_uuid import FOLIONamespaces

from libsys_airflow.plugins.folio.audit import AuditStatus, add_audit_log, _add_record

from libsys_airflow.plugins.folio.reports import inventory_audit_report


from test_remediate import mock_audit_database

from mocks import (  # noqa
    mock_dag_run,
    mock_file_system,
)


def test_get_inventory_audit_report(mock_dag_run, mock_file_system):  # noqa
    mock_audit_database(mock_dag_run, mock_file_system)

    db_connection = sqlite3.connect(mock_file_system[3] / "audit-remediation.db")

    exists_db_id = _add_record(
        {
            "id": "c6d21611-d279-421e-97ce-b72f40f94658",
            "hrid": "a34914",
            "_version": "1",
        },
        db_connection,
        FOLIONamespaces.instances.value,
    )

    add_audit_log(exists_db_id, db_connection, AuditStatus.EXISTS.value)

    for missing_record in [
        {
            "id": "d0d0edbf-e997-4496-ba53-32ec16660889",
            "hrid": "a56678",
            "_version": "1",
        },
        {
            "id": "80e5a714-7b99-4662-8c7d-ef846076ace2",
            "hrid": "a3990135",
            "_version": "1",
        },
    ]:
        missing_db_id = _add_record(
            missing_record, db_connection, FOLIONamespaces.instances.value
        )
        add_audit_log(missing_db_id, db_connection, AuditStatus.MISSING.value)

    inventory_audit_report(
        airflow=mock_file_system[0], iteration_id=mock_dag_run.run_id
    )

    with (mock_file_system[2] / "reports/report_inventory-audit.md").open() as fo:
        inventory_report = fo.read()

    assert "**Exists**: 1" in inventory_report
    assert "**Missing**: 2" in inventory_report
