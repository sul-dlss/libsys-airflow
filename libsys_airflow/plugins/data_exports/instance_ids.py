import csv
from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def fetch_record_ids(**kwargs) -> list:
    context = get_current_context()
    params = context.get("params", {})  # type: ignore
    airflow = kwargs.get("airflow", "/opt/airflow/libsys_airflow")
    sql_list = sql_files(params=params, airflow=airflow)
    results = []

    for idx, sqlfile in enumerate(sql_list):
        task_id = f"postgres_query_{idx}"
        with open(sqlfile) as sqf:
            query = sqf.read()

        from_date = params.get("from_date", datetime.now().strftime('%Y-%m-%d'))
        to_date = (datetime.now() + timedelta(1)).strftime('%Y-%m-%d')

        results.extend(
            SQLExecuteQueryOperator(
                task_id=task_id,
                conn_id="postgres_folio",
                database=kwargs.get("database", "okapi"),
                sql=query,
                parameters={
                    "from_date": from_date,
                    "to_date": to_date,
                },
            ).execute(context)
        )

    return results


def sql_files(**kwargs) -> list:
    sql_path = Path(kwargs.get("airflow", "/opt/airflow")) / "plugins/data_exports/sql"

    return list(sql_path.glob("*.sql"))


def save_ids_to_fs(**kwargs) -> str:
    airflow = kwargs.get("airflow", "/opt/airflow")
    task_instance = kwargs["task_instance"]
    vendor = kwargs["vendor"]
    data = task_instance.xcom_pull(task_ids="fetch_record_ids_from_folio")
    ids_path = save_ids(airflow=airflow, data=data, vendor=vendor)

    return ids_path

def save_ids(**kwargs) -> str:
    filestamp = kwargs.get("timestamp", datetime.now().strftime('%Y%m%d%H%M'))
    airflow = kwargs.get("airflow", "/opt/airflow")
    vendor = kwargs.get("vendor")
    data = kwargs.get("data", "")
    data_path = Path(airflow) / f"data-export-files/{vendor}/instanceids/{filestamp}.csv"
    data_path.parent.mkdir(parents=True, exist_ok=True)

    with open(data_path, 'w') as f:
        writer = csv.writer(f, lineterminator='\n')
        for id in data:
            if id:
                writer.writerow(id)

    return str(data_path)
