import json
import pathlib
import re
from io import BytesIO
from typing import Union

import pandas as pd

from airflow_client.client import DagRunApi, TriggerDAGRunPostBody
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.templating import Jinja2Templates

from libsys_airflow.plugins.data_exports.instance_ids import save_ids
from libsys_airflow.plugins.shared.airflow_api_client import api_client

app = FastAPI()

templates = Jinja2Templates(
    directory=[
        pathlib.Path(__file__).resolve().parent.parent.parent / "templates",
        pathlib.Path(__file__).resolve().parent.parent
        / "templates"
        / "data-export-upload",
    ]
)

parent = pathlib.Path(__file__).resolve().parent
vendor_file = open(parent / "vendors.json")
vendors = json.load(vendor_file)

uuid_regex = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)


def upload_data_export_ids(
    ids_df: pd.DataFrame, vendor: str, kind: str
) -> list[Union[str, int, None]]:
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


def _trigger_dag_run(vendor, kind, user_email, number_of_ids, filename):
    dag_id = f"select_{vendor}_records"
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        trigger_dag_run_post_body = TriggerDAGRunPostBody(
            conf={
                "fetch_folio_record_ids": False,
                "saved_record_ids_kind": kind,
                "user_email": user_email,
                "number_of_ids": number_of_ids,
                "uploaded_filename": filename,
            }
        )

        api_response = api_instance.trigger_dag_run(dag_id, trigger_dag_run_post_body)
    return api_response.dag_run_id


def _render_home(request: Request, messages: list[str] | None = None):
    return templates.TemplateResponse(
        request,
        "index.html",
        {"vendors": vendors["vendors"], "messages": messages or []},
    )


@app.get("/")
def data_export_upload_home(request: Request):
    return _render_home(request)


@app.post("/create")
def run_data_export_upload(
    request: Request,
    vendor: str = Form(default=""),  # noqa: B008
    kind: str = Form(default=""),  # noqa: B008
    user_email: str | None = Form(default=None),  # noqa: B008
    ids_file: UploadFile | None = File(default=None),  # noqa: B008
):
    if ids_file is None or not ids_file.filename:
        return _render_home(request, messages=["Missing Instance UUID File."])

    filename = ids_file.filename
    try:
        contents = ids_file.file.read()
        ids_df = pd.read_csv(BytesIO(contents), header=None)
        if not vendor:
            raise Exception("You must choose a vendor!")
        elif not kind:
            raise Exception(
                "You must select an option for New records, Updates or Deletes!"
            )
        number_of_ids = upload_data_export_ids(ids_df, vendor, kind).pop()
        messages = [f"Sucessfully uploaded ID file with {number_of_ids} IDs."]
        dag_run_id = _trigger_dag_run(vendor, kind, user_email, number_of_ids, filename)
        messages.append(f"Starting {vendor} DAG run {dag_run_id}.")
        return _render_home(request, messages=messages)
    except pd.errors.EmptyDataError:
        return _render_home(request, messages=["Warning! Empty UUID file."])
    except Exception as e:
        return _render_home(request, messages=[f"Error: {e}"])
