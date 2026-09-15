import logging
from datetime import datetime

from airflow.sdk import dag, get_current_context, task, Variable
from airflow.providers.standard.operators.empty import EmptyOperator

from libsys_airflow.plugins.folio.helpers.bw import (
    add_admin_notes,
    create_admin_note,
    create_bw_record,
    email_bw_summary,
    email_failure,
    post_bw_record,
)
from libsys_airflow.plugins.shared.folio_client import (
    UserTokenUnusable,
    folio_client_for_user,
)
from libsys_airflow.plugins.shared.user_token import (
    discard_user_token,
    read_user_token,
)

logger = logging.getLogger(__name__)


def _folio_token(params: dict) -> str:
    key = params.get("folio_token_key")
    if not key:
        raise UserTokenUnusable(
            "No FOLIO token for this run. The DAG acts in FOLIO as the user who "
            "triggered it, so trigger it from the Boundwith CSV Upload app."
        )

    token = read_user_token(key)
    if not token:
        # Discarded once the run ended. Clearing cannot mint another: a task has no
        # session, and a username alone cannot be exchanged for a token.
        raise UserTokenUnusable(
            "The FOLIO token for this run is gone. Clearing a finished run cannot get "
            "another, so trigger a new run from the Boundwith CSV Upload app."
        )
    return token


def _folio_client():
    params = get_current_context().get("params", {})  # type: ignore
    return folio_client_for_user(_folio_token(params))


@dag(
    schedule=None,
    start_date=datetime(2023, 11, 7),
    catchup=False,
    tags=["folio", "boundwith"],
    default_args={
        "email_on_failure": False,
    },
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
        # Fail here rather than after fanning out across every row.
        _folio_token(params)
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

    @task(trigger_rule="all_done")
    def discard_folio_token():
        """Runs however the rest of the run ended, so no token is left behind."""
        key = get_current_context().get("params", {}).get("folio_token_key")  # type: ignore
        if key:
            discard_user_token(key)

    start = EmptyOperator(task_id="start-bw-relationships")

    finished_bw_relationshps = EmptyOperator(task_id="finished-bw-relationships")

    rows = init_bw_relationships()

    admin_note = generate_admin_note()

    start >> rows >> admin_note

    bw_records = add_bw_record.expand(row=rows)

    (
        new_bw_record.expand(bw_parts=bw_records)
        >> [new_admin_notes(admin_note=admin_note), generate_emails()]
        >> finished_bw_relationshps
    )

    # Hangs off the end rather than sitting in the chain, so finished-bw-relationships
    # still only succeeds when the work did.
    finished_bw_relationshps >> discard_folio_token()


add_bw_relationships()
