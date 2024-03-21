import logging

from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from libsys_airflow.plugins.data_exports.instance_ids import save_ids

logger = logging.getLogger(__name__)


def fetch_full_dump_ids(**kwargs) -> None:
    airflow = kwargs.get("airflow", "/opt/airflow")
    batch_size = kwargs.get("batch_size", 50000)
    context = get_current_context()

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

        save_ids(
            airflow=airflow,
            vendor="full-dump",
            data=tuples,
            timestamp=f"{offset}_{batch}_ids",
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

    return int(result)


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
