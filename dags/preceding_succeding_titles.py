import json
import logging

from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.python import PythonOperator

from plugins.folio.login import folio_login
from plugins.folio.helpers import post_to_okapi, put_to_okapi

logger = logging.getLogger(__name__)

@dag(
    schedule_interval=None,
    start_date=datetime(2022, 6, 23),
    catchup=False,
    tags=["folio", "preceding_succeeding_titles"],
    max_active_runs=1,
)
def preceding_succeeding_titles():
    """
    ## Posts the json created by folio migration tools create_preceding_succeeding_titles method.
    After a successful completion of the convert_instances_valid_json in
    the symphony_marc_import DAG run, takes the json created by folio migration tools
    and POSTS to the Okapi /preceeding-succeding-titles endpoint
    """

    login = PythonOperator(
        task_id="folio_login", python_callable=folio_login
    )  # noqa

    @task
    def post_to_folio(**kwargs):
        pattern = "preceding_succeding_titles*.json"
        results = "/opt/airflow/migration/results"
        task_instance = kwargs["task_instance"]
        jwt = task_instance.xcom_pull(task_ids="folio_login")

        for file in Path(results).glob(pattern):
            logger.info(f"opening {file}")
            with open(file) as fo:
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
                        logger.warn(f"{result['errors'][0]['message']} -- trying a PUT instead")
                        put_to_okapi(
                            token=jwt,
                            records=obj,
                            endpoint=f"/preceding-succeeding-titles/{obj['id']}",
                            payload_key=None,
                        )


    @task
    def cleanup():
        context = get_current_context()
        _filename = context.get("params").get("filename_preceding_succeeding_titles")
        logger.info(f"Removing JSON file {_filename}")
        remove_json(_filename=_filename)

    @task
    def finish():
        context = get_current_context()
        _filename = context.get("params").get("filename_preceding_succeeding_titles")
        logger.info(f"Finished migration {_filename}")


    login >> post_to_folio() >> cleanup() >> finish()


def remove_json(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    _filename = kwargs["filename_preceding_succeeding_titles"]

    _filedir = Path(airflow) / 'migration/results/'
    for p in Path(_filedir).glob(f"{_filename}*"):
        p.unlink()
        logger.info(f"Removed {p}")


post_preceding_succesing_titles = preceding_succeeding_titles()
