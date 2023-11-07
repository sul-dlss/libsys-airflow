import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.folio.folio_client import FolioClient

from libsys_airflow.plugins.folio.helpers.bw import (
    create_bw_record,
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
)
def add_bw_relationships(**kwargs):
    """
    ## Creates Boundwith Relationships between Holdings and Items
    DAG is triggered by Plugin UI with an uploaded CSV
    """

    @task
    def start_bw_relationships(**kwargs) -> list:
        context = get_current_context()
        params = context.get("params")
        return params["relationships"]

    @task
    def add_bw_record(row: dict):
        folio_client = _folio_client()
        holdings_id = row['child_holdings_id']
        barcode = row["parent_barcode"]
        bw_parts = create_bw_record(
            folio_client=folio_client, holdings_id=holdings_id, barcode=barcode
        )
        return bw_parts

    @task
    def new_bw_record(**kwargs):
        bw_parts = kwargs["bw_parts"]
        task_instance = kwargs["ti"]
        folio_client = _folio_client()
        post_bw_record(
            folio_client=folio_client, bw_parts=bw_parts, task_instance=task_instance
        )

    finished_bw_relationshps = EmptyOperator(task_id="finished-bw-relationships")

    rows = start_bw_relationships()

    bw_records = add_bw_record.expand(row=rows)

    new_bw_record.expand(bw_parts=bw_records) >> finished_bw_relationshps


add_bw_relationships()
