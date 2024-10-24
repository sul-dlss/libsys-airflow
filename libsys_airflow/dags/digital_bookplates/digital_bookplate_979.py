from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.digital_bookplates.bookplates import (
    add_979_marc_tags,
    add_marc_tags_to_record,
    instance_id_for_druids,
    retrieve_druids_for_instance_task,
)


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
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

    druids_for_instance_id = retrieve_druids_for_instance_task()

    marc_tags_for_druid_instances = add_979_marc_tags(druids_for_instance_id)

    add_marc_tags = add_marc_tags_to_record(
        marc_instance_tags=marc_tags_for_druid_instances,
        instance_uuid=instance_id_for_druids(druid_instances=druids_for_instance_id),
    )

    (
        start
        >> druids_for_instance_id
        >> marc_tags_for_druid_instances
        >> add_marc_tags
        >> end
    )


digital_bookplate_979()
