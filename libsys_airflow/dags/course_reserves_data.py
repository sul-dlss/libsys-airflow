import logging

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.folio.courses import (
    current_next_term_ids,
    generate_course_reserves_data,
    generate_course_reserves_file,
)

logger = logging.getLogger(__name__)

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
        cron="40 01,11,16 * * *", timezone="America/Los_Angeles"
    ),
    start_date=datetime(2023, 8, 28),
    catchup=False,
    max_active_runs=1,
    tags=["folio", "courses"],
)
def course_reserves_data():
    retrieve_term_ids = current_next_term_ids()

    course_data = generate_course_reserves_data.expand(term_id=retrieve_term_ids)

    generate_course_reserves_file(course_data)


course_reserves_data()
