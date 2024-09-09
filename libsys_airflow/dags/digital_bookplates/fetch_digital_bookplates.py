from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.digital_bookplates.purl_fetcher import fetch_druids

default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 9, 9),
    catchup=False,
    tags=["digital bookplates"],
)
def fetch_digital_bookplates():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    fetch_bookplate_purls = fetch_druids()

    start >> fetch_bookplate_purls >> end


fetch_digital_bookplates()
