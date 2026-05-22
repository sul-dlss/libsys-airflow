import logging

from datetime import datetime, timedelta

from airflow.sdk import dag, Param
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.folio.reading_room import (
    retrieve_usergroup_lookup,
    retrieve_patron_group_lookup,
    retrieve_reading_rooms_lookup,
    retrieve_user_id_batches,
    process_user_id_batch,
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
        "user_batch_limit": Param(
            100,
            type="integer",
            minimum=1,
            maximum=1000,
            description="Number of user IDs to process per batch (1-1000, default: 100).",
        ),
    },
)
def reading_room_access():
    # Retrieve all lookup data as separate tasks
    usergroups = retrieve_usergroup_lookup()
    patron_groups = retrieve_patron_group_lookup()
    reading_rooms = retrieve_reading_rooms_lookup()

    # Retrieve user IDs in batches (lightweight - just IDs)
    user_id_batches = retrieve_user_id_batches()

    # Process each batch (dynamic task mapping)
    # Each task fetches its own user data and updates permissions
    process_user_id_batch.partial(
        usergroups=usergroups,
        patron_groups=patron_groups,
        reading_rooms=reading_rooms,
    ).expand(user_id_batch=user_id_batches)


reading_room_access()
