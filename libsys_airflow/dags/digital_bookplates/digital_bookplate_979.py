from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.digital_bookplates.bookplates import (
    launch_add_979_fields_task,
    construct_979_marc_tags,
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
    schedule=None,
    start_date=datetime(2023, 8, 28),
    catchup=False,
    tags=["digital bookplates"],
)
def digital_bookplate_979():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    instances_for_druids = launch_add_979_fields_task()

    # TODO
    marc_for_druid_instances = construct_979_marc_tags(instances_for_druids)

    # TODO
    add_marc_tags = {}

    # TODO
    email_979_report = {}

    (
        start
        >> instances_for_druids
        >> marc_for_druid_instances
        >> add_marc_tags
        >> email_979_report
        >> end
    )


digital_bookplate_979()
