import logging
import math

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.decorators import task, task_group

from libsys_airflow.plugins.data_exports.full_dump_marc import (
    fetch_number_of_records,
    fetch_full_dump_marc,
    refresh_view,
    reset_s3,
)
from libsys_airflow.plugins.data_exports.marc.transformer import Transformer
from libsys_airflow.plugins.data_exports.marc.transforms import remove_marc_fields
from sqlalchemy import exc

logger = logging.getLogger(__name__)


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    "select_all_records",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data export"],
    params={
        "batch_size": Param(
            5000,
            type="integer",
            description="Number of MARC records to process per file.",
        ),
        "concurrent_jobs": Param(
            5,
            type="integer",
            description="Number of batch processing jobs to run in parallel.",
        ),
    },
) as dag:

    start = EmptyOperator(task_id='start')

    @task
    def refresh_materialized_view():
        refresh_view()

    @task
    def reset_s3_bucket():
        reset_s3()

    @task
    def number_of_records():
        return fetch_number_of_records()

    @task
    def do_batch_size() -> int:
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        batch = params.get("batch_size", 5000)

        return int(batch)

    @task
    def do_concurrency() -> list[int]:
        context = get_current_context()
        params = context.get("params", {})  # type: ignore
        concurrency = params.get("concurrent_jobs", 5)

        return [i for i in range(int(concurrency))]

    @task
    def calculate_div(**kwargs):
        total = kwargs["number_of_records"]
        batch_size = kwargs["number_in_batch"]
        concurrent_jobs = kwargs["concurrent_jobs"]
        shard = batch_size * 10

        return math.ceil((total / len(concurrent_jobs)) / shard) * shard

    @task(multiple_outputs=True)
    def calculate_start_stop(div, job):
        output = {"start": int(div * job), "stop": int((job + 1) * div)}
        logger.info(f"Output in calculate_start_stop {output}")
        return output

    @task
    def fetch_folio_records(batch_size, start, stop):
        marc_file_list = []
        for offset in range(start, stop, batch_size):
            logger.info(f"fetch_folio_records: from {offset}")
            try:
                marc = fetch_full_dump_marc(offset=offset, batch_size=batch_size)
                marc_file_list.append(marc)
            except exc.OperationalError as err:
                logger.warning(f"{err} for offset {offset}")
                continue

        return marc_file_list

    @task_group(group_id="transform_marc")
    def marc_transformations(marc_files: list):
        @task
        def transform_marc_records_add_holdings(marc_files: list):
            transformer = Transformer()
            for marc_file in marc_files:
                transformer.add_holdings_items(marc_file=marc_file, full_dump=True)

        @task
        def transform_marc_records_remove_fields(marc_files: list):
            for marc_file in marc_files:
                remove_marc_fields(marc_file, full_dump=True)

        transform_marc_records_add_holdings(
            marc_files
        ) >> transform_marc_records_remove_fields(marc_files)

    number_of_jobs = do_concurrency()

    batch_size = do_batch_size()

    total_records = number_of_records()

    record_div = calculate_div(
        number_of_records=total_records,
        concurrent_jobs=number_of_jobs,
        number_in_batch=batch_size,
    )

    update_view = refresh_materialized_view()

    delete_s3_files = reset_s3_bucket()

    start_stop = calculate_start_stop.partial(div=record_div).expand(job=number_of_jobs)

    marc_file_list = fetch_folio_records.partial(batch_size=batch_size).expand_kwargs(
        start_stop
    )

    finish_transforms = marc_transformations.expand(marc_files=marc_file_list)

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )

    start >> update_view >> delete_s3_files >> total_records
    finish_transforms >> finish_processing_marc
