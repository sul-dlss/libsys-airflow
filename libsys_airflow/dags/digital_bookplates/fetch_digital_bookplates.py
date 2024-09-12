from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.digital_bookplates.purl_fetcher import (
    extract_bookplate_metadata,
    fetch_druids,
)

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
    schedule=CronDataIntervalTimetable(
        cron=Variable.get("digital_bookplates_run", "00 7 * * WED"),
        timezone="America/Los_Angeles",
    ),
    start_date=datetime(2024, 9, 9),
    catchup=False,
    tags=["digital bookplates"],
    render_template_as_native_obj=True,
)
def fetch_digital_bookplates():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    fetch_bookplate_purls = fetch_druids()

    bookplate_metadata = extract_bookplate_metadata.expand(
        druid_url=fetch_bookplate_purls
    )

    start >> fetch_bookplate_purls

    bookplate_metadata >> end


fetch_digital_bookplates()
