import json
import logging
import pathlib
from datetime import datetime, timezone

import pandas as pd

from airflow_client.client import DagRunApi, TriggerDAGRunPostBody
from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import Response

from libsys_airflow.plugins.shared.folio_client import folio_client
from libsys_airflow.plugins.shared.airflow_api_client import api_client
from libsys_airflow.plugins.shared.utils import (
    plugin_templates,
    redirect_with_query_params as _redirect,
)

logger = logging.getLogger(__name__)

app = FastAPI()

templates = plugin_templates(
    pathlib.Path(__file__).resolve().parent.parent, "circ_rules_tester"
)

CIRC_HOME = pathlib.Path("/opt/airflow/circ")


def _trigger_batch_dag_run(scenario_file) -> str:
    scenario_df = pd.read_csv(scenario_file)
    dag_id = "circ_rules_batch_tests"
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        trigger_dag_run_post_body = TriggerDAGRunPostBody(
            conf={"scenarios": scenario_df.to_json()}
        )

        api_response = api_instance.trigger_dag_run(dag_id, trigger_dag_run_post_body)
        return api_response.dag_run_id


@app.get("/")
def circ_home(request: Request):
    return templates.TemplateResponse(
        request,
        "index.html",
        {"message": request.query_params.get("message")},
    )


@app.post("/batch_test")
def run_batch_test(
    request: Request,
    upload_scenarios: UploadFile | None = File(default=None),  # noqa: B008
):
    if upload_scenarios is None or not upload_scenarios.filename:
        return templates.TemplateResponse(
            request,
            "index.html",
            {"message": "No scenario file uploaded"},
        )
    if not upload_scenarios.filename.endswith("csv"):
        return templates.TemplateResponse(
            request,
            "index.html",
            {"message": "Scenario file must be a csv"},
        )

    try:
        dag_run_id = _trigger_batch_dag_run(upload_scenarios.file)
        return _redirect(f"batch_report/{dag_run_id}")
    except Exception as e:
        return templates.TemplateResponse(
            request,
            "index.html",
            {"message": f"Failed to trigger circ_rules_batch_tests DAG. Error: {e} "},
        )


@app.post("/test")
def run_test(
    patron_group_id: str = Form(default=""),  # noqa: B008
    material_type_id: str = Form(default=""),  # noqa: B008
    loan_type_id: str = Form(default=""),  # noqa: B008
    location_id: str = Form(default=""),  # noqa: B008
):
    dag_id = "circ_rules_scenario_tests"
    trigger_dag_run_post_body = TriggerDAGRunPostBody(
        conf=dict(
            patron_group_id=patron_group_id,
            material_type_id=material_type_id,
            loan_type_id=loan_type_id,
            location_id=location_id,
        ),
    )
    with api_client() as airflow_api_client:
        api_instance = DagRunApi(airflow_api_client)
        try:
            api_response = api_instance.trigger_dag_run(
                dag_id, trigger_dag_run_post_body
            )
            run_id = api_response.dag_run_id
            return _redirect(f"report/{run_id}")
        except Exception as e:
            logger.error(f"Failed to Trigger circ_rules_scenario_test DAG, error:{e}")
            return _redirect(
                ".",
                message="Failed to Trigger circ_rules_scenario_test DAG",
            )


@app.get("/batch_report/{dag_run}")
def report_batch(dag_run: str, request: Request):
    batch_report_path = CIRC_HOME / f"{dag_run}.json"
    message = None
    if not batch_report_path.exists():
        message = f"Report for DAG Run not completed. DAG ID {dag_run}"
        report = None
    else:
        report = pd.read_json(batch_report_path, encoding="utf-8-sig")
    return templates.TemplateResponse(
        request,
        "batch_report.html",
        {"dag_run": dag_run, "report": report, "message": message},
    )


@app.get("/download/{dag_run}")
def download_report(dag_run: str):
    batch_report_path = CIRC_HOME / f"{dag_run}.json"
    if not batch_report_path.exists():
        return _redirect("..", message=f"Batch report DAG ID {dag_run} doesn't exist")
    report = pd.read_json(batch_report_path, encoding="utf-8-sig")
    timestamp = datetime.now(timezone.utc).toordinal()
    return Response(
        report.to_csv(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment;filename=batch_report_{timestamp}.csv"
        },
    )


@app.get("/reference")
@app.get("/reference/{data_type}")
def reference_data(request: Request, data_type: str | None = None):
    _folio_client = folio_client()
    is_download = bool(request.query_params.get("download"))
    match data_type:

        case "loan_type":
            title = "Loan Types"
            loan_types_df = pd.DataFrame(
                _folio_client.folio_get(
                    "loan-types", key="loantypes", query_params={"limit": 999}
                )
            )
            reference_df = loan_types_df.drop(columns=["metadata"]).rename(
                columns={"name": "FOLIO name", "id": "UUID"}
            )

        case "locations":
            title = "Locations"
            locations_df = pd.DataFrame(_folio_client.locations)
            drop_columns = [
                'discoveryDisplayName',
                'isActive',
                'institutionId',
                'campusId',
                'libraryId',
                'details',
                'primaryServicePoint',
                'servicePointIds',
                'servicePoints',
                'metadata',
                'description',
            ]
            if 'isShadow' in locations_df.columns:
                drop_columns.append('isShadow')
            reference_df = locations_df.drop(columns=drop_columns).rename(
                columns={"code": "FOLIO code", "name": "FOLIO name", "id": "UUID"}
            )

        case "material_type":
            title = "Material Types"
            material_types_df = pd.DataFrame(
                _folio_client.folio_get(
                    "material-types", key="mtypes", query_params={"limit": 999}
                )
            )
            reference_df = material_types_df.drop(
                columns=["source", "metadata"]
            ).rename(columns={"name": "FOLIO name", "id": "UUID"})

        case "patron_group":
            title = "Patron Groups"
            patron_group_df = pd.DataFrame(
                _folio_client.folio_get(
                    "/groups", key="usergroups", query_params={"limit": 999}
                )
            )
            reference_df = patron_group_df.drop(
                columns=["metadata", "expirationOffsetInDays"]
            ).rename(
                columns={"group": "FOLIO code", "desc": "FOLIO name", "id": "UUID"}
            )

        case _:
            title = "Reference Data"
            reference_df = pd.DataFrame()

    if is_download and not reference_df.empty:
        return Response(
            reference_df.to_csv(index=False),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment;filename={data_type}.csv"},
        )

    return templates.TemplateResponse(
        request,
        "reference-data.html",
        {"title": title, "data_type": data_type, "reference_df": reference_df},
    )


@app.get("/report/{dag_run}")
def report_scenario(dag_run: str, request: Request):
    scenario_report_path = CIRC_HOME / f"{dag_run}.json"
    message = None
    if not scenario_report_path.exists():
        message = f"Report for DAG Run not completed. DAG ID {dag_run}"
        report = None
    else:
        with scenario_report_path.open(encoding="utf-8-sig") as report_fo:
            report = json.load(report_fo)
    return templates.TemplateResponse(
        request,
        "report.html",
        {"dag_run": dag_run, "report": report, "message": message},
    )
