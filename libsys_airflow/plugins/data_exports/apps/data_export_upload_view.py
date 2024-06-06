import json
import pandas as pd
import pathlib
import re

from airflow.models import DagBag
from airflow.utils import timezone
from airflow.utils.state import State

from flask import flash, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from typing import Union

from libsys_airflow.plugins.data_exports.instance_ids import save_ids


parent = pathlib.Path(__file__).resolve().parent
vendor_file = open(parent / "vendors.json")
vendors = json.load(vendor_file)

uuid_regex = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def upload_data_export_ids(
    ids_df: pd.DataFrame, vendor: str, kind: str
) -> Union[str, None]:
    if len(ids_df.columns) > 1:
        raise ValueError("ID file has more than one column.")
    tuples = list(ids_df.itertuples(index=False, name=None))
    instance_uuids = []
    for row in tuples:
        id = row[0]
        if not uuid_regex.search(id):
            raise ValueError(f"{id} is not a UUID.")
        instance_uuids.append(id)

    ids_path = save_ids(
        airflow="/opt/airflow", vendor=vendor, data=instance_uuids, kind=kind
    )

    return ids_path


def default_rendered_page(self):
    return self.render_template(
        "data-export-upload/index.html", vendors=vendors['vendors']
    )


class DataExportUploadView(AppBuilderBaseView):
    default_view = "data_export_upload_home"
    route_base = "/data_export_upload"

    def _trigger_dag_run(self, vendor, kind):
        dagbag = DagBag("/opt/airflow/dags")
        dag = dagbag.get_dag(f"select_{vendor}_records")
        execution_date = timezone.utcnow()
        run_id = f"manual__{execution_date.isoformat()}"
        dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf={
                "fetch_folio_record_ids": False,
                "saved_record_ids_kind": kind,
            },
            external_trigger=True,
        )
        return run_id

    @expose("/create", methods=["POST"])
    def run_data_export_upload(self):
        if "upload-data-export-ids" not in request.files:
            flash("Missing Instance UUID File.")
            return default_rendered_page(self)
        else:
            try:
                raw_csv = request.files["upload-data-export-ids"]
                vendor = request.form.get("vendor")
                kind = request.form.get("kind")
                ids_df = pd.read_csv(raw_csv, header=None)
                if not vendor:
                    raise Exception("You must choose a vendor!")
                else:
                    upload_data_export_ids(ids_df, vendor, kind)
                    flash("Sucessfully uploaded ID file.")
                    dag_run_id = self._trigger_dag_run(vendor, kind)
                    flash(f"Starting {vendor} DAG run {dag_run_id}.")
            except pd.errors.EmptyDataError:
                flash("Warning! Empty UUID file.")
            except Exception as e:
                flash(f"Error: {e}")
            finally:
                return default_rendered_page(self)

    @expose("/")
    def data_export_upload_home(self):
        return default_rendered_page(self)
