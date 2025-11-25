import json
import pandas as pd
import pathlib
import re

from airflow.models import DagBag
from airflow.utils import timezone
from airflow.utils.state import State

from flask import flash, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

from libsys_airflow.plugins.data_exports.instance_ids import save_ids


parent = pathlib.Path(__file__).resolve().parent
vendor_file = open(parent / "vendors.json")
vendors = json.load(vendor_file)

uuid_regex = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def upload_data_export_ids(
    ids_df: pd.DataFrame, vendor: str, kind: str
) -> list[str, int]:
    if len(ids_df.columns) > 1:
        raise ValueError("ID file has more than one column.")
    tuples = list(ids_df.itertuples(index=False, name=None))
    instance_uuids = []
    for row in tuples:
        id = row[0]
        if not uuid_regex.search(id):
            raise ValueError(f"{id} is not a UUID.")
        instance_uuids.append(id)

    number_of_ids = len(instance_uuids)
    ids_path = save_ids(
        airflow="/opt/airflow", vendor=vendor, data=instance_uuids, kind=kind
    )

    return [ids_path, number_of_ids]


def default_rendered_page(self):
    return self.render_template(
        "data-export-upload/index.html", vendors=vendors['vendors']
    )


class DataExportUploadView(AppBuilderBaseView):
    default_view = "data_export_upload_home"
    route_base = "/data_export_upload"

    def _trigger_dag_run(self, vendor, kind, user_email, number_of_ids, filename):
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
                "email": user_email,
                "number_of_ids": number_of_ids,
                "filename": filename,
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
                elif not kind:
                    raise Exception(
                        "You must select an option for New records, Updates or Deletes!"
                    )
                else:
                    filename = raw_csv.filename
                    number_of_ids = upload_data_export_ids(ids_df, vendor, kind).pop()
                    flash(f"Sucessfully uploaded ID file with {number_of_ids} IDs.")
                    user_email = request.form.get("user_email")
                    dag_run_id = self._trigger_dag_run(
                        vendor, kind, user_email, number_of_ids, filename
                    )
                    flash(f"Starting {vendor} DAG run {dag_run_id}.")
            except pd.errors.EmptyDataError:
                flash("Warning! Empty UUID file.")
            except Exception as e:
                flash(f"Error: {e}")
            finally:
                page = default_rendered_page(self)  # noqa
            return page

    @expose("/")
    def data_export_upload_home(self):
        return default_rendered_page(self)
