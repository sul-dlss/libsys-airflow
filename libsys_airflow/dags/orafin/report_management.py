import logging

from datetime import datetime

from airflow.decorators import dag

from airflow.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable

from libsys_airflow.plugins.orafin.reports import (
    find_reports,
    retrieve_reports,
    remove_reports,
)

from libsys_airflow.plugins.orafin.tasks import (
    consolidate_reports_task,
    get_new_reports_task,
    filter_files_task,
    launch_report_processing_task,
)

logger = logging.getLogger(__name__)


@dag(
    schedule=CronDataIntervalTimetable(
        cron="00 18 * * 2,4", timezone="America/Los_Angeles"
    ),
    start_date=datetime(2023, 10, 20),
    catchup=False,
    tags=["folio", "orafin"],
)
def ap_report_management():
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    ap_reports = filter_files_task()

    new_reports = get_new_reports_task()

    retrieve_new_reports = retrieve_reports().expand(env=new_reports)

    all_reports = consolidate_reports_task()

    launch_dag_runs = launch_report_processing_task()

    remove_all_reports = remove_reports().expand(env=all_reports)

    start >> find_reports() >> ap_reports
    ap_reports >> new_reports >> retrieve_new_reports
    retrieve_new_reports >> [all_reports, launch_dag_runs]
    launch_dag_runs >> end
    all_reports >> remove_all_reports >> end


ap_report_management()
