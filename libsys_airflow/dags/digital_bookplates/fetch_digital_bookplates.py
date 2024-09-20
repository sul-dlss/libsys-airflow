from datetime import datetime, timedelta

from airflow.decorators import dag, task_group
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.digital_bookplates.email import (
    deleted_from_argo_email,
    bookplates_metadata_email,
    missing_fields_email,
)

from libsys_airflow.plugins.digital_bookplates.purl_fetcher import (
    add_update_model,
    check_deleted_from_argo,
    extract_bookplate_metadata,
    fetch_druids,
    filter_updates_errors,
)


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@task_group(group_id="retrieve-process-db")
def extract_db_process_group(druid_url: str):
    metadata = extract_bookplate_metadata(druid_url)
    return add_update_model(metadata)


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

    db_results = extract_db_process_group.expand(druid_url=fetch_bookplate_purls)

    deleted_druids = check_deleted_from_argo(fetch_bookplate_purls)

    filtered_data = filter_updates_errors(db_results)

    missing_fields_email(failures=filtered_data['failures'])

    start >> fetch_bookplate_purls >> db_results

    (
        db_results
        >> deleted_druids
        >> deleted_from_argo_email(deleted_druids=deleted_druids)
        >> bookplates_metadata_email(
            new=db_results["new"], updated=db_results["updated"]
        )
        >> end
    )


fetch_digital_bookplates()
