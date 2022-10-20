import json
import logging

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator

from plugins.folio.login import folio_login
from plugins.folio.helpers import post_to_okapi, put_to_okapi


logger = logging.getLogger(__name__)


"""
    ## Posts the json created by folio migration tools create_preceding_succeeding_titles method.
    After a successful completion of the convert_instances_valid_json in
    the symphony_marc_import DAG run, takes the json created by folio migration tools
    and POSTS to the Okapi /preceeding-succeding-titles endpoint
"""


def post_to_folio(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow/")
    context = get_current_context()
    params = context.get("params")
    iteration_id = params["iteration_id"]
    task_instance = kwargs["task_instance"]
    jwt = task_instance.xcom_pull(task_ids="folio_login")
    preceding_succeeding_titles_path = Path(airflow) / f"migration/iterations/{iteration_id}/results/preceding_succeeding_titles.json"

    logger.info(f"Opening {preceding_succeeding_titles_path}")
    with preceding_succeeding_titles_path.open() as fo:
        for obj in fo.readlines():
            obj = json.loads(obj)

            logger.info(f"Posting {obj}")
            result = post_to_okapi(
                token=jwt,
                records=obj,
                endpoint="/preceding-succeeding-titles",
                payload_key=None,
                **kwargs,
            )

            if "errors" in result:
                logger.warn(
                    f"{result['errors'][0]['message']} -- trying a PUT instead"
                )
                put_to_okapi(
                    token=jwt,
                    records=obj,
                    endpoint=f"/preceding-succeeding-titles/{obj['id']}",
                    payload_key=None,
                )


def finish():
    context = get_current_context()
    _filename = context.get("params").get("filename_preceding_succeeding_titles")
    logger.info(f"Finished migration {_filename}")


with DAG(
    "process_preceding_succeeding_titles",
    schedule_interval=None,
    start_date=datetime(2022, 6, 23),
    catchup=False,
    tags=["folio", "preceding_succeeding_titles"],
    max_active_runs=1,
) as dag:

    login = PythonOperator(task_id="folio_login", python_callable=folio_login)  # noqa

    preceding_succeeding_titles = PythonOperator(
        task_id="load_preceding_succeeding_titles",
        python_callable=post_to_folio,
    )

    wrap_up = PythonOperator(task_id="log_finished", python_callable=finish)


login >> preceding_succeeding_titles >> wrap_up
