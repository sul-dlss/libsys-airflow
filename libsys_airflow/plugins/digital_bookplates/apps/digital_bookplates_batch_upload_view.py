import logging
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from flask import flash, redirect, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from sqlalchemy.orm import Session

from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate


logger = logging.getLogger(__name__)


def _trigger_add_979_dags(**kwargs):
    """Placeholder to trigger DAG"""
    instance_uuid = kwargs["instance_uuid"]
    fund = kwargs["fund"]
    dag_payload = {instance_uuid: fund}
    logger.info(f"Triggers 979 DAG with payload {dag_payload}")


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
            for row in upload_instances_df.iterrows():
                instance_uuid = row[1]
                _trigger_add_979_dags(instance_uuid=instance_uuid, fund=fund)

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
