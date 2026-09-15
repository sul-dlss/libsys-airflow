import pathlib
from typing import Union

import pandas as pd

from airflow.api_fastapi.core_api.security import GetUserDep
from fastapi import Depends, FastAPI, File, Form, Request, UploadFile

from airflow_client.client import DagRunApi, TriggerDAGRunPostBody
from libsys_airflow.plugins.shared.airflow_api_client import api_client
from libsys_airflow.plugins.shared.auth import current_access_token, require_view_access
from libsys_airflow.plugins.shared.csrf import CSRFCookieMiddleware, csrf_protect
from libsys_airflow.plugins.shared.user_token import store_user_token
from libsys_airflow.plugins.shared.utils import plugin_templates

app = FastAPI(
    openapi_url=None,
    dependencies=[Depends(require_view_access("Boundwith CSV Upload"))],
)
app.add_middleware(CSRFCookieMiddleware)

templates = plugin_templates(pathlib.Path(__file__).resolve().parent, "boundwith")


def trigger_bw_dag(
    bw_df: pd.DataFrame,
    sunid: str,
    user_email: Union[str, None],
    file_name: str,
    folio_token: Union[str, None],
) -> str:
    dag_id = "add_bw_relationships"
    # Only the key: the token itself would be readable by any user from the run details.
    folio_token_key = store_user_token(folio_token) if folio_token else None
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        trigger_dag_run_post_body = TriggerDAGRunPostBody(
            conf={
                "relationships": bw_df.to_dict(orient='records'),
                "email": user_email,
                "sunid": sunid,
                "file_name": file_name,
                "folio_token_key": folio_token_key,
            }
        )

        api_response = api_instance.trigger_dag_run(dag_id, trigger_dag_run_post_body)

    return api_response.dag_run_id


@app.get("/")
def bw_home(request: Request):
    return templates.TemplateResponse(request, "index.html", {})


@app.post("/create", dependencies=[Depends(csrf_protect)])
def run_bw_creation(
    request: Request,
    user: GetUserDep,
    folio_token: str | None = Depends(current_access_token),  # noqa: B008
    user_email: str | None = Form(default=None),  # noqa: B008
    upload_boundwith: UploadFile | None = File(default=None),  # noqa: B008
):
    # Taken from the session rather than a form field, which the user could set to
    # anyone. It is what the DAG writes into the admin note.
    sunid = user.get_name()

    if upload_boundwith is None or not upload_boundwith.filename:
        return templates.TemplateResponse(
            request,
            "index.html",
            {"message": "Missing Boundwith Relationship File"},
        )

    try:
        bw_df = pd.read_csv(upload_boundwith.file)
        if ["part_holdings_hrid", "principle_barcode"] != list(bw_df.columns):
            return templates.TemplateResponse(
                request,
                "index.html",
                {"message": f"Invalid columns: {list(bw_df.columns)} for CSV file"},
            )
        elif len(bw_df) < 2:
            return templates.TemplateResponse(
                request,
                "index.html",
                {
                    "message": "Warning! CSV file only contains one row. Need to include row for the principle's barcode and holdings HRID."
                },
            )
        elif len(bw_df) > 1_000:
            return templates.TemplateResponse(
                request,
                "index.html",
                {"message": f"Warning! CSV file has {len(bw_df)} rows, limit is 1,000"},
            )
        else:
            run_id = trigger_bw_dag(
                bw_df, sunid, user_email, upload_boundwith.filename, folio_token
            )
            return templates.TemplateResponse(
                request,
                "index.html",
                {"run_id": run_id, "user_email": user_email},
            )
    except pd.errors.EmptyDataError:
        return templates.TemplateResponse(
            request,
            "index.html",
            {"message": "Warning! Empty CSV file for Boundwith Relationship DAG"},
        )
    except Exception as e:
        return templates.TemplateResponse(
            request, "index.html", {"message": f"Error with CSV {e}"}
        )
