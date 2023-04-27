import pathlib
import sqlite3

import pandas as pd

from jinja2 import Template

from folio_uuid.folio_uuid import FOLIONamespaces

from libsys_airflow.plugins.folio.audit import AuditStatus


def _get_record_info(folio_record_type: int, cur: sqlite3.Cursor):
    """
    Audit Status
    """
    output = {
        "total": cur.execute(TOTAL_SQL, (folio_record_type,)).fetchone()[0],
        "exists": cur.execute(
            AUDIT_STATUS_SQL, (folio_record_type, AuditStatus.EXISTS.value)
        ).fetchone()[0],
        "missing": cur.execute(
            AUDIT_STATUS_SQL, (folio_record_type, AuditStatus.MISSING.value)
        ).fetchone()[0],
        "errors": cur.execute(
            AUDIT_STATUS_SQL, (folio_record_type, AuditStatus.ERROR.value)
        ).fetchone()[0],
        "added": cur.execute(
            AUDIT_STATUS_SQL, (folio_record_type, AuditStatus.ADDED.value)
        ).fetchone()[0],
    }
    return output


def _error_table(con: sqlite3.Connection) -> str:
    """
    Generates Markdown table for errors
    """
    df_errors = pd.read_sql(
        ERRORS_SQL,
        con,
    )
    df_errors = df_errors.rename(
        columns={
            "uuid": "Record ID",
            "name": "Folio Type",
            "http_status_code": "HTTP Error Code",
            "message": "Error Message",
        }
    )
    return df_errors.to_markdown()


def inventory_audit_report(**kwargs):
    """
    Generates an Inventory Audit Report
    """
    airflow = kwargs.get("airflow", "/opt/airflow")

    iteration = kwargs['iteration']
    iteration_path = pathlib.Path(airflow) / f"migration/iterations/{iteration}"

    report_path = iteration_path / "reports/report_inventory-audit.md"
    report_template = Template(
        (pathlib.Path(__file__).parent / "templates/folio/inventory.tmpl").read_text()
    )

    db_connection = sqlite3.connect(iteration_path / "results/audit-remediation.db")

    cur = db_connection.cursor()

    audit_report = report_template.render(
        {
            "instances": _get_record_info(FOLIONamespaces.instances.value, cur),
            "holdings": _get_record_info(FOLIONamespaces.holdings.value, cur),
            "items": _get_record_info(FOLIONamespaces.items.value, cur),
            "errors": _error_table(db_connection),
        }
    )

    cur.close()
    report_path.write_text(audit_report)


def srs_audit_report(db_connection: sqlite3.Connection, iteration: str):
    """
    Generates a SRS Audit Report
    """
    report_path = pathlib.Path(iteration) / "reports/report_srs-audit.md"
    report_template = report_template = Template(
        (pathlib.Path(__file__).parent / "templates/folio/srs.tmpl").read_text()
    )

    cur = db_connection.cursor()

    audit_report = report_template.render(
        {
            "total_mhld_srs_records": cur.execute(
                TOTAL_SQL, (FOLIONamespaces.srs_records_holdingsrecord.value,)
            ).fetchone()[0],
            "total_bib_srs_records": cur.execute(
                TOTAL_SQL, (FOLIONamespaces.srs_records_bib.value,)
            ).fetchone()[0],
            "exists_mhld": cur.execute(
                AUDIT_STATUS_SQL,
                (
                    FOLIONamespaces.srs_records_holdingsrecord.value,
                    AuditStatus.EXISTS.value,
                ),
            ).fetchone()[0],
            "missing_mhld": cur.execute(
                AUDIT_STATUS_SQL,
                (
                    FOLIONamespaces.srs_records_holdingsrecord.value,
                    AuditStatus.MISSING.value,
                ),
            ).fetchone()[0],
            "error_mhld": cur.execute(
                AUDIT_STATUS_SQL,
                (
                    FOLIONamespaces.srs_records_holdingsrecord.value,
                    AuditStatus.ERROR.value,
                ),
            ).fetchone()[0],
            "exists_bibs": cur.execute(
                AUDIT_STATUS_SQL,
                (FOLIONamespaces.srs_records_bib.value, AuditStatus.EXISTS.value),
            ).fetchone()[0],
            "missing_bibs": cur.execute(
                AUDIT_STATUS_SQL,
                (FOLIONamespaces.srs_records_bib.value, AuditStatus.MISSING.value),
            ).fetchone()[0],
            "error_bibs": cur.execute(
                AUDIT_STATUS_SQL,
                (FOLIONamespaces.srs_records_bib.value, AuditStatus.ERROR.value),
            ).fetchone()[0],
        }
    )
    cur.close()
    report_path.write_text(audit_report)


AUDIT_STATUS_SQL = """SELECT count(AuditLog.id) FROM AuditLog, Record
WHERE AuditLog.record_id = Record.id AND
Record.folio_type=? AND
AuditLog.status=?;"""
ERRORS_SQL = """SELECT Record.uuid, FolioType.name, Errors.http_status_code, Errors.message
FROM Errors, FolioType, Record, AuditLog
WHERE Errors.log_id = AuditLog.id
AND AuditLog.record_id = Record.id
AND Record.folio_type = FolioType.id;"""
TOTAL_SQL = """SELECT count(id) FROM Record where folio_type=?;"""
