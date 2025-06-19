import csv
import logging

from datetime import datetime
from pathlib import Path
from typing import Union

from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)


def create_sal3_items_view(**kwargs) -> Union[str, None]:
    context = get_current_context()
    query = None

    logger.info("Refreshing materialized view for sal3 items")
    with open(sal3_items_sql_file()) as sqv:
        query = sqv.read()

    SQLExecuteQueryOperator(
        task_id="postgres_full_count_query",
        conn_id="postgres_folio",
        database=kwargs.get("database", "okapi"),
        sql=query,
    ).execute(context)

    return query


def sal3_items_sql_file(**kwargs) -> Path:
    sql_path = (
        Path(kwargs.get("airflow", "/opt/airflow"))
        / "libsys_airflow/plugins/data_exports/sql/sal3_items.sql"
    )

    return sql_path


def folio_items_to_csv(**kwargs) -> str:
    """
    returns tuples:
    (
        (
            item_uuid,
            holding_id,
            item_barcode,
            item_status,
            item_permanent_location,
            item_temporary_location,
            holdings_permanent_location,
            item_temporary_location,
            item_suppressed
        ),
        (...)
    )
    """
    connection = kwargs.get("connection")
    cursor = connection.cursor()  # type: ignore
    sql = "select * from sal3_items;"
    cursor.execute(sql)
    tuples = cursor.fetchall()

    airflow = kwargs.get("airflow", "/opt/airflow")
    sal3_items_path = Path(airflow) / "data-export-files/caiasoft/csv/sal3_items"
    sal3_items_path.mkdir(parents=True, exist_ok=True)
    sal3_items_csv_file = Path(
        sal3_items_path / f"folio_sync_{datetime.now().strftime('%Y-%m')}.csv"
    )

    with open(sal3_items_csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                'item_uuid',
                'holdings_id',
                'item_barcode',
                'item_status',
                'item_permanent_location',
                'item_temporary_location',
                'holdings_permanent_location',
                'item_temporary_location',
                'item_suppressed',
            ]
        )
        writer.writerows(tuples)

    return str(sal3_items_csv_file)


def gather_items_csv_for_caiasoft(**kwargs) -> dict:
    file_list = []
    sal3_items_csv_path = kwargs.get("sal3_items_csv_path", "")
    file_list.append(sal3_items_csv_path)

    return {"file_list": file_list}


def pick_success_files(files_dict) -> str:
    return str(files_dict['success'])
