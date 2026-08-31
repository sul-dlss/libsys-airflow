import pathlib

import pandas as pd

from fastapi import Depends, FastAPI, File, Form, Request, UploadFile

from airflow_client.client import DagRunApi, TriggerDAGRunPostBody

from libsys_airflow.plugins.shared.airflow_api_client import api_client
from libsys_airflow.plugins.shared.csrf import CSRFCookieMiddleware, csrf_protect
from libsys_airflow.plugins.shared.utils import plugin_templates

app = FastAPI()
app.add_middleware(CSRFCookieMiddleware)

templates = plugin_templates(
    pathlib.Path(__file__).resolve().parent.parent, "deletes-csv-upload"
)


def _save_deletes_csv(deletes_df: pd.DataFrame, filename: str) -> str:
    authority_uploads_path = pathlib.Path("/opt/airflow/authorities/uploads")
    authority_uploads_path.mkdir(parents=True, exist_ok=True)
    deletes_csv_path = authority_uploads_path / filename
    deletes_df.to_csv(deletes_csv_path, index=False)
    return str(deletes_csv_path.absolute())


def _trigger_dag_run(deletes_csv_file: str, email: str | None = None) -> str:
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        trigger_body = TriggerDAGRunPostBody(
            conf={"kwargs": {"file": deletes_csv_file, "email": email}}
        )
        api_response = api_instance.trigger_dag_run(
            "delete_authority_records", trigger_body
        )
        return api_response.dag_run_id


@app.get("/")
def authorities_delete_home(request: Request):
    return templates.TemplateResponse(request, "index.html", {})


@app.post("/upload", dependencies=[Depends(csrf_protect)])
def upload_csv(
    request: Request,
    email: str | None = Form(default=None),  # noqa: B008
    upload_deletes: UploadFile | None = File(default=None),  # noqa: B008
):
    if upload_deletes is None or not upload_deletes.filename:
        return templates.TemplateResponse(
            request, "index.html", {"message": "Missing file upload"}
        )

    try:
        deletes_csv_df = pd.read_csv(upload_deletes.file, names=["001s"])
        deletes_csv_file = _save_deletes_csv(deletes_csv_df, upload_deletes.filename)
        run_id = _trigger_dag_run(deletes_csv_file, email)
        return templates.TemplateResponse(
            request, "index.html", {"run_id": run_id, "email": email}
        )
    except pd.errors.EmptyDataError:
        return templates.TemplateResponse(
            request, "index.html", {"message": "Upload csv file is empty"}
        )
    except Exception as e:
        return templates.TemplateResponse(
            request, "index.html", {"message": f"Error with upload: {e}"}
        )
