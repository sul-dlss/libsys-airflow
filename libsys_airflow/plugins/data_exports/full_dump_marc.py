import logging

from datetime import datetime, timedelta
from pathlib import Path
from s3path import S3Path
from typing import Union

from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from libsys_airflow.plugins.data_exports.marc.exporter import Exporter

logger = logging.getLogger(__name__)


def create_materialized_view(**kwargs) -> Union[str, None]:
    context = get_current_context()
    params = context.get("params", {})  # type: ignore
    recreate = params.get("recreate_view", False)
    from_date = params.get("from_date")
    to_date = params.get(
        "to_date", (datetime.now() + timedelta(1)).strftime('%Y-%m-%d')
    )

    query = None
    if recreate:
        with open(materialized_view_sql_file()) as sqf:
            query = sqf.read()

        SQLExecuteQueryOperator(
            task_id="postgres_full_count_query",
            conn_id="postgres_folio",
            database=kwargs.get("database", "okapi"),
            sql=query,
            parameters={
                "from_date": from_date,
                "to_date": to_date,
            },
        ).execute(context)
    else:
        logger.info("Skipping refresh of materialized view")

    return query


def materialized_view_sql_file(**kwargs) -> Path:
    sql_path = (
        Path(kwargs.get("airflow", "/opt/airflow"))
        / "libsys_airflow/plugins/data_exports/sql/materialized_view.sql"
    )

    return sql_path


def fetch_full_dump_marc(**kwargs) -> str:
    offset = kwargs.get("offset")
    batch_size = kwargs.get("batch_size", 1000)
    connection = kwargs.get("connection")
    cursor = connection.cursor()  # type: ignore
    sql = "SELECT id, hrid, content FROM public.data_export_marc ORDER BY hrid LIMIT (%s) OFFSET (%s)"
    params = (batch_size, offset)
    cursor.execute(sql, params)
    tuples = cursor.fetchall()

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
    ).execute(
        context
    )  # type: ignore

    count = result[0][0]
    logger.info(f"Record count: {count}")
    return int(count)


def reset_s3(**kwargs) -> None:
    context = get_current_context()
    params = context.get("params", {})  # type: ignore
    reset = params.get("reset_s3", True)

    if reset:
        bucket = Variable.get("FOLIO_AWS_BUCKET", "folio-data-export-prod")
        s3_dir = S3Path(f"/{bucket}/data-export-files/full-dump/marc-files/")
        s3_files = s3_dir.glob("*.*")
        for file in s3_files:
            file.unlink()
    else:
        logger.info("Skipping deletion of existing marc files in S3 bucket")

    return None
