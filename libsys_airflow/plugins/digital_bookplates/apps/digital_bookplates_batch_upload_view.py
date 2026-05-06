from datetime import datetime, timezone
import logging
import pathlib

import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook

from flask import flash, redirect, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from sqlalchemy.orm import Session

from libsys_airflow.plugins.digital_bookplates.bookplates import (
    launch_digital_bookplate_979_dag,
    launch_poll_for_979_dags_email,
)
from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate


logger = logging.getLogger(__name__)


def _save_uploaded_file(files_base: str, file_name: str, upload_df: pd.DataFrame):
    """
    Saves uploaded file to digital-bookplates/{year}/{day} location
    and if file name already exists, increments until unique
    """
    current_time = datetime.now(timezone.utc)
    report_base = (
        pathlib.Path(files_base)
        / f"{current_time.year}/{current_time.month}/{current_time.day}"
    )
    report_base.mkdir(parents=True, exist_ok=True)

    report_path = report_base / file_name

    while report_path.exists():
        count_str = report_path.stem.split("copy-")[-1]
        try:
            count = int(count_str)
            old_count = f"copy-{count}"
            count += 1
            name = report_path.stem.replace(old_count, f"copy-{count}")
        except ValueError:
            count = 1
            name = f"{report_path.stem}-copy-{count}"
        report_path = report_path.with_name(f"{name}{report_path.suffix}")
    upload_df.to_csv(report_path, index=False)


def _get_fund(fund_id: int) -> dict:
    if not fund_id:
        return None

    pg_hook = PostgresHook("digital_bookplates")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        fund = session.query(DigitalBookplate).get(fund_id)
    return {
        "druid": fund.druid,
        "fund_name": fund.fund_name,
        "image_filename": fund.image_filename,
        "title": fund.title,
    }


class DigitalBookplatesBatchUploadView(AppBuilderBaseView):
    default_view = "digital_bookplates_batch_upload_home"
    route_base = "/digital_bookplates_batch_upload"
    files_base = "digital-bookplates"

    @expose("/create", methods=["POST"])
    def trigger_add_979_dags(self):
        if "upload-instance-uuids" not in request.files:
            flash("Missing Instance UUIDs file")
            return redirect(f"/pluginsv2/{self.route_base}/")

        email = request.form.get("email")

        fund_db_id = request.form.get("fundSelect")
        if not fund_db_id:
            flash("Fund not selected!")
            return redirect(f"/pluginsv2/{self.route_base}/")

        fund = _get_fund(fund_db_id)
        if fund is None:
            flash("Invalid fund selected")
            return redirect(f"/pluginsv2/{self.route_base}/")

        raw_upload_instances_file = request.files.get("upload-instance-uuids")
        if len(raw_upload_instances_file.filename) < 1:
            flash("Missing Instance UUIDs file")
            return redirect(f"/pluginsv2/{self.route_base}/")
        if not raw_upload_instances_file.filename.endswith("csv"):
            flash("Instance UUIDs file must be a csv")
            return redirect(f"/pluginsv2/{self.route_base}/")

        try:
            df = pd.read_csv(raw_upload_instances_file, header=None)
            if df.empty:
                flash("Warning! Empty Instance UUID file.")
                return redirect(f"/pluginsv2/{self.route_base}/")

            upload_instances_df = df.rename(columns={0: 'Instance UUID'})
            dag_runs = []
            for row in upload_instances_df.iterrows():
                instance_uuid = row[1][0]
                dag_run_id = launch_digital_bookplate_979_dag(
                    instance_uuid=instance_uuid, funds=[fund]
                )
                dag_runs.append(dag_run_id)
            _save_uploaded_file(
                DigitalBookplatesBatchUploadView.files_base,
                raw_upload_instances_file.filename,
                upload_instances_df,
            )
            launch_poll_for_979_dags_email(dag_runs=dag_runs, email=email)
            flash(
                f"Triggered {len(dag_runs)} DAG run(s) for {raw_upload_instances_file.filename}"
            )
        except pd.errors.EmptyDataError:
            flash("Warning! Empty Instance UUID file.")
        return redirect(f"/pluginsv2/{self.route_base}/")

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
