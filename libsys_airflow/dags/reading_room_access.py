import logging

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.models import Variable
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.folio.reading_room import (
    retrieve_users_for_reading_room_access,
    generate_reading_room_access,
    update_reading_room_permissions,
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
    start_date=datetime(2026, 1, 10),
    catchup=False,
    max_active_runs=1,
    tags=["folio", "reading_room"],
    params={
        "from_date": Param(
            Variable.get("FOLIO_EPOCH_DATE", "1885-11-11"),
            format="date",
            type="string",
            description="The earliest date to select record IDs from FOLIO.",
        ),
    },
)
def reading_room_access():
    retrieve_users = retrieve_users_for_reading_room_access()
    generate_access = generate_reading_room_access(retrieve_users)
    update_reading_room_permissions(generate_access)


reading_room_access()
