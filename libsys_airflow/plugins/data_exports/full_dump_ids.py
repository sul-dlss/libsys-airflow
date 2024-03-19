import logging

from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from libsys_airflow.plugins.data_exports.instance_ids import save_ids

logger = logging.getLogger(__name__)


def fetch_full_dump_ids(**kwargs) -> None:
    results: list[str] = []
    batch_size = kwargs.get("batch_size", 50000)
    context = get_current_context()

    total = fetch_number_of_records()
    batch = round(total/batch_size)
    i = 0

    query = f"select id from public.data_export_marc_ids limit %(batch_size)s offset %(offset)s"

    while i<=batch:
        task_id = f"postgres_full_dump_query_{i}"
        offset=i*batch

        tuples = SQLExecuteQueryOperator(
            task_id=task_id,
            conn_id="postgres_folio",
            database=kwargs.get("database", "okapi"),
            sql=query,
            parameters={
                "offset": offset,
                "batch_size": batch
            }
        ).execute(context)
        i+=1

        save_ids(airflow="/opt/airflow", vendor="full-dump", data=tuples, filestamp=f"{offset}_{batch}_ids.csv")


def fetch_number_of_records(**kwargs) -> int:
    context = get_current_context()

    query = f"select count(id) from public.data_export_marc_ids"

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
