import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator

from folioclient import FolioClient

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
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
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
        task_instance = kwargs["ti"]
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        task_instance.xcom_push(key="user_email", value=params.get("email"))
        task_instance.xcom_push(key="sunid", value=params['sunid'])
        task_instance.xcom_push(key="file_name", value=params["file_name"])
        return params.get("relationships", [])  # type: ignore

    @task
    def add_bw_record(row: dict):
        folio_client = _folio_client()
        holdings_hrid = row['part_holdings_hrid']
        barcode = row["principle_barcode"]
        bw_parts = create_bw_record(
            folio_client=folio_client, holdings_hrid=holdings_hrid, barcode=barcode
        )
        return bw_parts

    @task
    def generate_admin_note(**kwargs):
        task_instance = kwargs['ti']
        sunid = task_instance.xcom_pull(task_ids="init_bw_relationships", key="sunid")
        admin_note = create_admin_note(sunid)
        return admin_note

    @task
    def generate_emails(**kwargs):
        task_instance = kwargs["ti"]
        devs_email_addr = Variable.get("EMAIL_DEVS")
        email_bw_summary(devs_email_addr, task_instance)

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
