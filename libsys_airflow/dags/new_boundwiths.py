import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from folioclient import FolioClient

from libsys_airflow.plugins.folio.helpers.bw import check_add_bw, discover_bw_parts_files

logger = logging.getLogger(__name__)

folio_client = FolioClient(
    Variable.get("okapi_url"),
    "sul",
    Variable.get("folio_user"),
    Variable.get("folio_password"),
)

default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

parallel_posts = Variable.get("parallel_posts", 3)

with DAG(
    "boundwith_relationships",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 2, 22),
    catchup=False,
    tags=["bib_import", "folio"],
    max_active_runs=1,
) as dag:

    discovery_bw_files = PythonOperator(
        task_id="discovery-bw-parts",
        python_callable=discover_bw_parts_files,
        op_kwargs={"jobs": parallel_posts},
    )

    start_checks_add = EmptyOperator(task_id="start-check-add")

    finished_checks_add = EmptyOperator(task_id="finished-check-add")

    for i in range(int(parallel_posts)):
        check_add_relationships = PythonOperator(
            task_id=f"check-add-{i}",
            python_callable=check_add_bw,
            op_kwargs={"job": i, "folio_client": folio_client}
        )

        start_checks_add >> check_add_relationships >> finished_checks_add

    discovery_bw_files >> start_checks_add
