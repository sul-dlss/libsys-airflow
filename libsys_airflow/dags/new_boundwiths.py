import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.folio.folio_client import FolioClient

from libsys_airflow.plugins.folio.helpers.bw import (
    add_admin_notes,
    create_admin_note,
    create_bw_record,
    email_bw_summary,
    email_failure,
    post_bw_record,
)

logger = logging.getLogger(__name__)


def _folio_client():
    return FolioClient(
        Variable.get("okapi_url"),
        "sul",
        Variable.get("migration_user"),
        Variable.get("migration_password"),
    )


@dag(
    schedule=None,
    start_date=datetime(2023, 11, 7),
    catchup=False,
    tags=["folio", "boundwith"],
    on_failure_callback=email_failure,
)
def add_bw_relationships(**kwargs):
    """
    ## Creates Boundwith Relationships between Holdings and Items
    DAG is triggered by Plugin UI with an uploaded CSV
    """

    @task
    def init_bw_relationships(**kwargs) -> list:
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        return params.get("relationships", [])  # type: ignore

    @task
    def add_bw_record(row: dict):
        folio_client = _folio_client()
        holdings_hrid = row['child_holdings_hrid']
        barcode = row["parent_barcode"]
        bw_parts = create_bw_record(
            folio_client=folio_client, holdings_hrid=holdings_hrid, barcode=barcode
        )
        return bw_parts

    @task
    def generate_admin_note(**kwargs):
        context = get_current_context()
        params = context.get("params")
        admin_note = create_admin_note(params['sunid'])
        return admin_note

    @task
    def generate_emails(**kwargs):
        task_instance = kwargs["ti"]
        context = get_current_context()
        params = context.get("params")
        user_email = params.get("email")
        devs_email_addr = Variable.get("ORAFIN_TO_EMAIL_DEVS")
        email_bw_summary(user_email, devs_email_addr, task_instance)

    @task
    def new_bw_record(**kwargs):
        bw_parts = kwargs["bw_parts"]
        task_instance = kwargs["ti"]
        folio_client = _folio_client()
        post_bw_record(
            folio_client=folio_client, bw_parts=bw_parts, task_instance=task_instance
        )

    @task
    def new_admin_notes(**kwargs):
        note = kwargs["admin_note"]
        task_instance = kwargs['ti']
        folio_client = _folio_client()
        add_admin_notes(note, task_instance, folio_client)

    start = EmptyOperator(task_id="start-bw-relationships")

    finished_bw_relationshps = EmptyOperator(task_id="finished-bw-relationships")

    rows = init_bw_relationships()

    admin_note = generate_admin_note()

    start >> [rows, admin_note]

    bw_records = add_bw_record.expand(row=rows)

    (
        new_bw_record.expand(bw_parts=bw_records)
        >> [new_admin_notes(admin_note=admin_note), generate_emails()]
        >> finished_bw_relationshps
    )


add_bw_relationships()
