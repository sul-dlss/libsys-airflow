import logging

from s3path import S3Path

from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from libsys_airflow.plugins.data_exports.marc.exporter import Exporter

logger = logging.getLogger(__name__)


def fetch_full_dump_marc(**kwargs) -> str:
    context = get_current_context()
    offset = kwargs.get("offset")
    batch_size = kwargs.get("batch_size", 1000)
    task_id = f"postgres_full_dump_query_{offset}"

    query = "SELECT id, content FROM public.data_export_marc LIMIT %(limit)s OFFSET %(offset)s"

    tuples = SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id="postgres_folio",
        database=kwargs.get("database", "okapi"),
        sql=query,
        parameters={"offset": offset, "limit": batch_size},
    ).execute(context)

    exporter = Exporter()
    marc_file = exporter.retrieve_marc_for_full_dump(
        f"{offset}_{offset + batch_size}.mrc",
        instance_ids=tuples,
    )

    return str(marc_file)


def fetch_number_of_records(**kwargs) -> int:
    context = get_current_context()

    query = "SELECT count(id) from public.data_export_marc"

    result = SQLExecuteQueryOperator(
        task_id="postgres_full_count_query",
        conn_id="postgres_folio",
        database=kwargs.get("database", "okapi"),
        sql=query,
    ).execute(context)

    count = result[0][0]
    logger.info(f"Record count: {count}")
    return int(count)


def refresh_view(**kwargs) -> None:
    context = get_current_context()
    params = context.get("params", {})  # type: ignore
    refresh = params.get("refresh_view", True)

    if refresh:
        query = "refresh materialized view data_export_marc"

        result = SQLExecuteQueryOperator(
            task_id="postgres_full_count_query",
            conn_id="postgres_folio",
            database=kwargs.get("database", "okapi"),
            sql=query,
        ).execute(context)

        logger.info(result)
    else:
        logger.info("Skipping refresh of materialized view")

    return None


def reset_s3(**kwargs) -> None:
    context = get_current_context()
    params = context.get("params", {})  # type: ignore
    reset = params.get("reset_s3", True)

    if reset:
        bucket = Variable.get("FOLIO_AWS_BUCKET", "folio-data-export-prod")
        s3_files = S3Path(f"/{bucket}/data-export-files/full-dump")
        for file in s3_files:
            file.unlink()
    else:
        logger.info("Skipping deletion of existing marc files in S3 bucket")

    return None
