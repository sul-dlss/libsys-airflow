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


def create_campus_filter_view(**kwargs) -> Union[str, None]:
    context = get_current_context()
    params = context.get("params", {})  # type: ignore
    recreate = params.get("recreate_view", False)
    include_campus = params.get("include_campus", "SUL, LAW, GSB, HOOVER, MED")

    campuses = add_quotes(include_campus)
    # TODO: Use SQL param biding instead of inline query.
    # query = None

    query = (
        "DROP MATERIALIZED VIEW IF EXISTS filter_campus_ids CASCADE; "  # noqa
        "create materialized view filter_campus_ids as "
        "select distinct(instanceid) from sul_mod_inventory_storage.holdings_record "
        "where  permanentlocationid in ( "
        "    select id from sul_mod_inventory_storage.location "
        "    where campusid in ( "
        "        select id from sul_mod_inventory_storage.loccampus "
        f"        where jsonb->>'code' in ({campuses}) "
        "    )"
        ");"
        "DROP INDEX IF EXISTS filter_full_dump_campus_idx; "
        "CREATE UNIQUE INDEX filter_full_dump_campus_idx ON filter_campus_ids (instanceid);"
    )

    if recreate:
        logger.info(f"Refreshing view filter with campus codes {campuses}")

        # TODO: Use SQL param biding instead of inline query.
        # with open(filter_campus_sql_file()) as sqv:
        #     query = sqv.read()

        SQLExecuteQueryOperator(
            task_id="postgres_full_count_query",
            conn_id="postgres_folio",
            database=kwargs.get("database", "okapi"),
            sql=query,
        ).execute(context)
    else:
        logger.info("Skipping refresh of campus filter view")

    return query


def create_materialized_view(**kwargs) -> Union[str, None]:
    context = get_current_context()
    params = context.get("params", {})  # type: ignore
    recreate = params.get("recreate_view", False)
    from_date = params.get("from_date", '2023-08-23')
    to_date = params.get(
        "to_date", (datetime.now() + timedelta(1)).strftime('%Y-%m-%d')
    )

    query = None

    if recreate:
        logger.info(
            f"Refreshing materialized view with dates from: {from_date} to: {to_date}"
        )
        with open(materialized_view_sql_file()) as sqv:
            query = sqv.read()

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


def filter_campus_sql_file(**kwargs) -> Path:
    sql_path = (
        Path(kwargs.get("airflow", "/opt/airflow"))
        / "libsys_airflow/plugins/data_exports/sql/filter_campus_ids.sql"
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


def add_quotes(csv_string):
    """Adds single quotes around each comma-separated value in a string."""
    values = csv_string.split(',')
    quoted_values = ["'{}'".format(v.strip()) for v in values]
    return ','.join(quoted_values)
