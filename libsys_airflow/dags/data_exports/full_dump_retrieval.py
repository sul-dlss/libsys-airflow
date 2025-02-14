import logging
import math
import pathlib

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.decorators import task, task_group

from libsys_airflow.plugins.data_exports.full_dump_marc import (
    create_materialized_view,
    fetch_number_of_records,
    fetch_full_dump_marc,
    reset_s3,
)
from libsys_airflow.plugins.data_exports.marc.transformer import Transformer
from libsys_airflow.plugins.data_exports.sql_pool import SQLPool
from libsys_airflow.plugins.data_exports.marc.transforms import (
    marc_clean_serialize,
    zip_marc_file,
)
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
        "exclude_tags": Param(
            True,
            type="boolean",
            description="Remove excluded tags listed in marc/excluded_tags.pyfrom incoming record.",
        ),
        "from_date": Param(
            Variable.get("FOLIO_EPOCH_DATE", "2023-08-23"),
            format="date",
            type="string",
            description="The earliest date to select record IDs from FOLIO.",
        ),
        "to_date": Param(
            f"{(datetime.now()).strftime('%Y-%m-%d')}",
            format="date",
            type="string",
            description="The latest date to select record IDs from FOLIO.",
        ),
        "recreate_view": Param(
            False,
            type="boolean",
            description="Recreate the materialized view with the original FOLIO marc records to process.",
        ),
    },
) as dag:

    start = EmptyOperator(task_id='start')

    @task
    def create_full_selection_matrerialized_view():
        create_materialized_view()

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
        _connection = connection_pool.getconn()
        marc_file_list = []

        for offset in range(start, stop, batch_size):
            logger.info(f"fetch_folio_records: from {offset}")
            try:
                marc = fetch_full_dump_marc(
                    offset=offset, batch_size=batch_size, connection=_connection
                )
                marc_file_list.append(marc)
            except exc.OperationalError as err:
                logger.warning(f"{err} for offset {offset}")
                continue

        connection_pool.putconn(_connection, close=True)  # type: ignore
        return marc_file_list

    @task_group(group_id="transform_marc")
    def marc_transformations(marc_files: list):
        @task
        def transform_marc_records_add_holdings(marc_files: list):
            _connection = connection_pool.getconn()
            transformer = Transformer(connection=_connection)

            for marc_file in marc_files:
                transformer.add_holdings_items(marc_file=marc_file, full_dump=True)

            connection_pool.putconn(_connection, close=True)

        @task
        def transform_marc_records_clean_serialize(marc_files: list):
            context = get_current_context()
            params = context.get("params", {})  # type: ignore
            exclude_tags = params.get("exclude_tags", True)
            for marc_file in marc_files:
                marc_clean_serialize(
                    marc_file, full_dump=True, exclude_tags=exclude_tags
                )

        @task
        def compress_marc_files(marc_files: list):
            for marc_file in marc_files:
                stem = pathlib.Path(marc_file).suffix
                xml = marc_file.replace(stem, '.xml')
                zip_marc_file(xml, True)

        (
            transform_marc_records_add_holdings(marc_files)
            >> transform_marc_records_clean_serialize(marc_files)
            >> compress_marc_files(marc_files)
        )

    connection_pool = SQLPool().pool()

    number_of_jobs = do_concurrency()

    batch_size = do_batch_size()

    total_records = number_of_records()

    record_div = calculate_div(
        number_of_records=total_records,
        concurrent_jobs=number_of_jobs,
        number_in_batch=batch_size,
    )

    create_view = create_full_selection_matrerialized_view()

    delete_s3_files = reset_s3_bucket()

    start_stop = calculate_start_stop.partial(div=record_div).expand(job=number_of_jobs)

    marc_file_list = fetch_folio_records.partial(batch_size=batch_size).expand_kwargs(
        start_stop
    )

    finish_transforms = marc_transformations.expand(marc_files=marc_file_list)

    finish_processing_marc = EmptyOperator(
        task_id="finish_marc",
    )

    start >> create_view >> delete_s3_files >> total_records
    finish_transforms >> finish_processing_marc
