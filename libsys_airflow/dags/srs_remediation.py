import logging
import pathlib
import sqlite3

import requests

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context

from folioclient import FolioClient
from folio_uuid.folio_uuid import FOLIONamespaces

from libsys_airflow.plugins.folio.helpers.marc import get_snapshot_id, srs_check_add
from libsys_airflow.plugins.folio.reports import srs_audit_report

logger = logging.getLogger(__name__)


def _get_audit_db(results_dir: str) -> sqlite3.Connection:
    audit_db_path = pathlib.Path(results_dir) / "audit-remediation.db"
    if not audit_db_path.exists():
        raise ValueError(f"{audit_db_path} does not exist")
    return sqlite3.connect(audit_db_path)


def _folio_client():
    return FolioClient(
        Variable.get("okapi_url"),
        "sul",
        Variable.get("migration_user"),
        Variable.get("migration_password"),
    )


with DAG(
    "srs_audit_checks",
    default_args={
        "owner": "folio",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule=None,
    start_date=datetime(2023, 3, 2),
    catchup=False,
    tags=["bib_import", "folio"],
    max_active_runs=4,
) as dag:

    @task(multiple_outputs=True)
    def start_srs_check_remediation(**kwargs):
        context = kwargs.get("context")
        if context is None:
            context = get_current_context()
        params = context.get("params")
        iteration = params.get("iteration")
        results_dir = f"{iteration}/results/"
        logger.info(f"{iteration}")
        # return results_dir, iteration
        return {"results_dir": results_dir, "iteration": iteration}

    @task
    def get_snapshot() -> str:
        return get_snapshot_id(_folio_client())

    @task
    def start_mhld_check_add(**kwargs):
        results_dir = pathlib.Path(kwargs['results_dir'])

        audit_connection = _get_audit_db(results_dir)
        mhld_count = srs_check_add(
            audit_connection=audit_connection,
            results_dir=results_dir,
            srs_type=FOLIONamespaces.srs_records_holdingsrecord.value,
            file_name="folio_srs_holdings_mhld-transformer.json",
            snapshot_id=kwargs["snapshot"],
            folio_client=_folio_client(),
            srs_label="SRS MHLDs",
        )
        audit_connection.close()
        return mhld_count

    @task
    def start_bib_check_add(**kwargs):
        snapshot = kwargs["snapshot"]

        results_dir = pathlib.Path(kwargs['results_dir'])

        audit_connection = _get_audit_db(results_dir)
        bib_count = srs_check_add(
            audit_connection=audit_connection,
            results_dir=results_dir,
            srs_type=FOLIONamespaces.srs_records_bib.value,
            file_name="folio_srs_instances_bibs-transformer.json",
            snapshot_id=snapshot,
            folio_client=_folio_client(),
            srs_label="SRS MARC BIBs",
        )
        audit_connection.close()
        return bib_count

    @task
    def generate_audit_report(**kwargs):
        iteration = kwargs["iteration"]
        results_dir = kwargs["results_dir"]
        logger.info(
            f"Generating Audit Report for {kwargs['mhld_count']:,} MHLDs and {kwargs['bib_count']} bib records"
        )
        audit_connection = _get_audit_db(results_dir)
        srs_audit_report(audit_connection, iteration)
        audit_connection.close()
        return f"Finished SRS Audit Report for {iteration}"

    @task
    def complete_snapshot(**kwargs):
        folio_client = _folio_client()
        snapshot = kwargs["snapshot"]
        snapshot_completed = requests.put(
            f"{folio_client.okapi_url}/source-storage/snapshots/{snapshot}",
            headers=folio_client.okapi_headers,
            json={"jobExecutionId": snapshot, "status": "COMMITTED"},
        )
        return snapshot_completed.status_code

    params = start_srs_check_remediation()

    snapshot = get_snapshot()

    mhld_count = start_mhld_check_add(
        results_dir=params['results_dir'], snapshot=snapshot
    )

    bib_count = start_bib_check_add(
        results_dir=params['results_dir'], snapshot=snapshot
    )

    audit_msg = generate_audit_report(
        iteration=params['iteration'],
        results_dir=params['results_dir'],
        mhld_count=mhld_count,
        bib_count=bib_count,
    )

    complete_snapshot(snapshot=snapshot, mhld_count=mhld_count, bib_count=bib_count)
