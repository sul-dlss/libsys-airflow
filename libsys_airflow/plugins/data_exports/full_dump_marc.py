import logging

from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from libsys_airflow.plugins.data_exports.marc.exporter import Exporter

logger = logging.getLogger(__name__)


def fetch_full_dump_marc(**kwargs) -> None:
    context = get_current_context()
    params = context.get("params", {})  # type: ignore
    batch_size = params.get("batch_size", 50000)
    total = fetch_number_of_records()
    batch = round(total / batch_size)
    i = 0

    query = (
        "SELECT id FROM public.data_export_marc_ids LIMIT %(limit)s OFFSET %(offset)s"
    )

    while i <= batch - 1:
        task_id = f"postgres_full_dump_query_{i}"
        offset = i * batch_size

        tuples = SQLExecuteQueryOperator(
            task_id=task_id,
            conn_id="postgres_folio",
            database=kwargs.get("database", "okapi"),
            sql=query,
            parameters={"offset": offset, "limit": batch_size},
        ).execute(context)
        i += 1

        exporter = Exporter()
        exporter.retrieve_marc_for_full_dump(
            f"{offset}_{offset + batch_size}.mrc", instance_ids=tuples
        )


def fetch_number_of_records(**kwargs) -> int:
    context = get_current_context()

    query = "SELECT count(id) from public.data_export_marc_ids"

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

    query = "refresh materialized view data_export_marc_ids"

    result = SQLExecuteQueryOperator(
        task_id="postgres_full_count_query",
        conn_id="postgres_folio",
        database=kwargs.get("database", "okapi"),
        sql=query,
    ).execute(context)

    logger.info(result)

    return None
