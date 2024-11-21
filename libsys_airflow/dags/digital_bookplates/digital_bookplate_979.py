from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from libsys_airflow.plugins.digital_bookplates.bookplates import (
    add_979_marc_tags,
    add_marc_tags_to_record,
    check_979_action,
    delete_979_marc_tags,
    remove_marc_tags_from_record,
)


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def instance_id_for_druids(**kwargs) -> list:
    params = kwargs.get("params", {})
    druid_instances = params.get("druids_for_instance_id", {})
    if druid_instances is None:
        return []
    return list(druid_instances.keys())


@dag(
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 8, 28),
    catchup=False,
    max_active_runs=5,
    tags=["digital bookplates"],
)
def digital_bookplate_979():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    check_action = check_979_action()

    marc_add_for_druid_instances = add_979_marc_tags()

    marc_delete_for_druid_instances = delete_979_marc_tags()

    instance_id = instance_id_for_druids()

    add_marc_tags = add_marc_tags_to_record(
        marc_instance_tags=marc_add_for_druid_instances,
        instance_uuid=instance_id,
    )

    delete_marc_tags = remove_marc_tags_from_record(
        marc_instance_tags=marc_delete_for_druid_instances,
        instance_uuid=instance_id,
    )

    start >> check_action
    check_action >> [marc_add_for_druid_instances, marc_delete_for_druid_instances]

    marc_add_for_druid_instances >> add_marc_tags >> end
    marc_delete_for_druid_instances >> delete_marc_tags >> end


digital_bookplate_979()
