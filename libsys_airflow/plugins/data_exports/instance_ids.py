import ast
import logging
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union

from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)


def choose_fetch_folio_ids(**kwargs):
    fetch_records = kwargs.get("fetch_folio_record_ids", True)
    if isinstance(fetch_records, str):
        fetch_records = ast.literal_eval(fetch_records)

    if fetch_records:
        return "fetch_record_ids_from_folio"
    else:
        return "save_ids_to_file"


def fetch_record_ids(**kwargs) -> dict:
    context = get_current_context()
    params = context.get("params", {})  # type: ignore
    airflow = kwargs.get("airflow", "/opt/airflow/libsys_airflow")
    results = {"new": [], "updates": [], "deletes": []}  # type: dict

    for kind in ["new", "updates", "deletes"]:
        sql_list = sql_files(params=params, airflow=airflow, kind=kind)

        for idx, sqlfile in enumerate(sql_list):
            task_id = f"postgres_query_{idx}"
            with open(sqlfile) as sqf:
                query = sqf.read()

            from_date = params.get("from_date", datetime.now().strftime('%Y-%m-%d'))
            to_date = params.get(
                "to_date", (datetime.now() + timedelta(1)).strftime('%Y-%m-%d')
            )

            results[kind].extend(
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

        results[kind] = list(np.unique(results[kind]))

    return results


def sql_files(**kwargs) -> list:
    kind = kwargs.get("kind")
    sql_path = (
        Path(kwargs.get("airflow", "/opt/airflow")) / f"plugins/data_exports/sql/{kind}"
    )

    return list(sql_path.glob("*.sql"))


def save_ids_to_fs(**kwargs) -> list[Union[str, None]]:
    ids_path: list[str | None] = []
    airflow = kwargs.get("airflow", "/opt/airflow")
    task_instance = kwargs["task_instance"]
    vendor = kwargs["vendor"]
    kind = kwargs.get("record_id_kind", "")
    data = task_instance.xcom_pull(task_ids="fetch_record_ids_from_folio")

    if kind:
        data_path = Path(airflow) / f"data-export-files/{vendor}/instanceids/{kind}/"
        for file in data_path.glob("*.csv"):
            ids_path.append(str(file))
    else:
        for kind in ["new", "updates", "deletes"]:
            ids = save_ids(airflow=airflow, data=data[kind], kind=kind, vendor=vendor)
            ids_path.append(ids)

    return ids_path


def save_ids(**kwargs) -> Union[str, None]:
    filestamp = kwargs.get("timestamp", datetime.now().strftime('%Y%m%d%H%M'))
    airflow = kwargs.get("airflow", "/opt/airflow")
    vendor = kwargs.get("vendor")
    data = kwargs.get("data")
    kind = kwargs.get("kind")

    if not data:
        logger.info(f"No new changes for {vendor} record {kind}")
        return None

    data_path = (
        Path(airflow) / f"data-export-files/{vendor}/instanceids/{kind}/{filestamp}.csv"
    )
    data_path.parent.mkdir(parents=True, exist_ok=True)

    with open(data_path, 'w') as f:
        for id in data:
            f.write(f"{id}\n")

    return str(data_path)
