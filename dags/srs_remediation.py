import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from folioclient import FolioClient

from plugins.folio.helpers.marc import discover_srs_files, handle_srs_files

logger = logging.getLogger(__name__)

FOLIO_CLIENT = FolioClient(
    Variable.get("OKAPI_URL"),
    "sul",
    Variable.get("FOLIO_USER"),
    Variable.get("FOLIO_PASSWORD"),
)

parallel_posts = Variable.get("parallel_posts", 3)

with DAG(
    "srs_audit_checks",
    default_args={
        "owner": "folio",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2023, 3, 2),
    catchup=False,
    tags=["bib_import", "folio"],
    max_active_runs=1,
) as dag:

    discovery_srs = PythonOperator(
        task_id="find-srs-files",
        python_callable=discover_srs_files,
        op_kwargs={"jobs": parallel_posts},
    )

    start_srs_check_add = EmptyOperator(task_id="start-srs-check-add")

    finished_srs_check_add = EmptyOperator(task_id="end-srs-check-add")

    for i in range(parallel_posts):
        check_add_srs = PythonOperator(
            task_id=f"extract-check-add-{i}",
            python_callable=handle_srs_files,
            op_kwargs={"job": i, "folio_client": FOLIO_CLIENT},
        )

        start_srs_check_add >> check_add_srs >> finished_srs_check_add

    discovery_srs >> start_srs_check_add
