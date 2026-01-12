import logging

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.param import Param
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.folio.reading_room import (
    retrieve_users_batch_for_reading_room_access,
    generate_reading_room_access,
    update_reading_room_permissions,
    ReadingRoomsData,
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
        cron="30 3 * * *", timezone="America/Los_Angeles"
    ),
    start_date=datetime(2025, 12, 31),
    catchup=False,
    max_active_runs=1,
    tags=["folio", "reading_room"],
    params={
        "from_date": Param(
            None,
            format="date",
            type=["null", "string"],
            description="The earliest date to select record IDs from FOLIO.",
        ),
    },
)
def reading_room_access():
    reading_rooms_data = ReadingRoomsData()
    retrieved_users = retrieve_users_batch_for_reading_room_access()
    generate_access = generate_reading_room_access(
        users=retrieved_users, reading_rooms_data=reading_rooms_data
    )
    update_reading_room_permissions(generate_access)


reading_room_access()
