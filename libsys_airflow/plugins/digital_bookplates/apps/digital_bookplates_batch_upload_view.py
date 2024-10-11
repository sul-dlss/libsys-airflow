import datetime
import logging
import pathlib

import pandas as pd

from airflow.models import DagBag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.state import State

from flask import flash, redirect, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from sqlalchemy.orm import Session

from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate


logger = logging.getLogger(__name__)


def _trigger_add_979_dags(**kwargs) -> str:
    """Triggers add_bookplate_970 DAG"""
    instance_uuid = kwargs["instance_uuid"]
    fund = kwargs["fund"]
    dag_payload = {instance_uuid: fund}
    dagbag = DagBag("/opt/airflow/dags")
    dag = dagbag.get_dag("digital_bookplate_979")

    execution_date = timezone.utcnow()
    run_id = f"manual__{execution_date.isoformat()}"
    dag.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        state=State.RUNNING,
        conf={"druids_for_instance_id": dag_payload},
        external_trigger=True,
    )
    logger.info(f"Triggers 979 DAG with dag_id {run_id}")
    return run_id


def _save_uploaded_file(files_base: str, file_name: str, upload_df: pd.DataFrame):
    """
    Saves uploaded file to digital-bookplates/{year}/{day} location
    and if file name already exists, increments until unique
    """
    current_time = datetime.datetime.utcnow()
    report_path = (
        pathlib.Path(files_base) / f"{current_time.year}/{current_time.day}/{file_name}"
    )
    count: int = 1
    while report_path.exists():
        report_path = report_path.with_name(
            f"{report_path.stem}-{count}{report_path.suffix}"
        )
        count += 1
    upload_df.to_csv(report_path, index=False)


def _get_fund(fund_id: int) -> dict:
    pg_hook = PostgresHook("digital_bookplates")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        fund = session.query(DigitalBookplate).get(fund_id)
    return {"druid": fund.druid, "title": fund.title, "fund_name": fund.fund_name}


class DigitalBookplatesBatchUploadView(AppBuilderBaseView):
    default_view = "digital_bookplates_batch_upload_home"
    route_base = "/digital_bookplates_batch_upload"
    files_base = "digital-bookplates"

    @expose("/create", methods=["POST"])
    def trigger_add_975_dags(self):
        if "upload-instances-file" not in request.files:
            flash("Missing Instance UUIDs file")
            return redirect('/digital_bookplates_batch_upload')

        email = request.form.get("email")
        fund_db_id = request.form.get("fundId")
        fund = _get_fund(fund_db_id)
        raw_upload_instances_file = request.files["upload-instances-file"]
        try:
            upload_instances_df = pd.read_csv(raw_upload_instances_file, header=None)
            dag_runs = []
            for row in upload_instances_df.iterrows():
                instance_uuid = row[1]
                dag_run_id = _trigger_add_979_dags(
                    instance_uuid=instance_uuid, fund=fund
                )
                dag_runs.append(dag_run_id)
            _save_uploaded_file(
                DigitalBookplatesBatchUploadView.files_base,
                raw_upload_instances_file.filename,
                upload_instances_df,
            )
        except pd.errors.EmptyDataError:
            flash("Warning! Empty Instance UUID file.")
            return redirect('/digital_bookplates_batch_upload')

    @expose("/")
    def digital_bookplates_batch_upload_home(self):
        pg_hook = PostgresHook("digital_bookplates")
        with Session(pg_hook.get_sqlalchemy_engine()) as session:
            digital_bookplates = (
                session.query(DigitalBookplate)
                .order_by(DigitalBookplate.fund_name)
                .all()
            )

        return self.render_template(
            "digital_bookplates/index.html", digital_bookplates=digital_bookplates
        )
