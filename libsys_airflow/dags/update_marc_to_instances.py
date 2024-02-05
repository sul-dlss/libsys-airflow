import logging

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.folio.db import initilize_marc_to_instances_db
from libsys_airflow.plugins.folio.helpers import calculate_batches
from libsys_airflow.plugins.folio.helpers.marc import process_srs_records
from libsys_airflow.plugins.folio.instances import get_instances_ids

logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2024, 2, 2),
    catchup=False,
    schedule=None,
    tags=["folio", "update"],
)
def marc_to_instances_updates():
    finished = EmptyOperator(task_id="finished")

    @task
    def start_task(**kwargs):
        context = get_current_context()
        params = context.get("params")
        # Default batch size is 5,000
        batch_size = params.get("batch_size", "5000")
        return batch_size

    @task
    def setup_results_db_task(**kwargs):
        return initilize_marc_to_instances_db(**kwargs)

    @task
    def calculate_batches_task(batch_size: str):
        return calculate_batches(batch_size)

    @task(max_active_tis_per_dagrun=5)
    def retrieve_instances_info_task(db_path: str, batch_number: int, ti=None):
        batch_size = ti.xcom_pull(task_ids='start_task')
        return get_instances_ids(int(batch_number), int(batch_size), db_path)

    @task(max_active_tis_per_dagrun=5)
    def process_srs_record_task(batch_number: int, ti=None):
        db_path = ti.xcom_pull(task_ids="setup_results_db_task")
        process_srs_records(batch_number, db_path)

    batch_size = start_task()

    batches = calculate_batches_task(batch_size)

    db_path = setup_results_db_task(batches=batches)

    batch_number = retrieve_instances_info_task.partial(db_path=db_path).expand(
        batch_number=batches
    )

    process_srs_record_task(batch_number) >> finished


marc_to_instances_updates()
