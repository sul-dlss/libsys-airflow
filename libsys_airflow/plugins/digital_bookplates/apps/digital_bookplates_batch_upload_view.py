import logging
import pathlib
from datetime import datetime, timezone
from io import BytesIO
from urllib.parse import urlencode

import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from libsys_airflow.plugins.digital_bookplates.bookplates import (
    launch_digital_bookplate_979_dag,
    launch_poll_for_979_dags_email,
)
from libsys_airflow.plugins.digital_bookplates.models import DigitalBookplate


logger = logging.getLogger(__name__)

app = FastAPI()

templates = Jinja2Templates(
    directory=pathlib.Path(__file__).resolve().parent.parent
    / "templates"
    / "digital_bookplates"
)

files_base = "digital-bookplates"


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


def _get_fund(fund_id) -> dict | None:
    if not fund_id:
        return None

    pg_hook = PostgresHook("digital_bookplates")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        fund = session.query(DigitalBookplate).get(fund_id)
    return {
        "druid": fund.druid,  # type: ignore
        "fund_name": fund.fund_name,  # type: ignore
        "image_filename": fund.image_filename,  # type: ignore
        "title": fund.title,  # type: ignore
    }


def _redirect_home(**query_params) -> RedirectResponse:
    return RedirectResponse(url=f".?{urlencode(query_params)}", status_code=303)


@app.get("/")
def digital_bookplates_batch_upload_home(request: Request):
    pg_hook = PostgresHook("digital_bookplates")
    with Session(pg_hook.get_sqlalchemy_engine()) as session:
        digital_bookplates = (
            session.query(DigitalBookplate).order_by(DigitalBookplate.fund_name).all()
        )

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "digital_bookplates": digital_bookplates,
            "message": request.query_params.get("message"),
        },
    )


@app.post("/create")
def trigger_add_979_dags(
    request: Request,
    email: str | None = Form(default=None),  # noqa: B008
    fund_select: str | None = Form(default=None),  # noqa: B008
    upload_instance_uuids: UploadFile | None = File(default=None),  # noqa: B008
):
    if upload_instance_uuids is None or not upload_instance_uuids.filename:
        return _redirect_home(message="Missing Instance UUIDs file")

    if not fund_select:
        return _redirect_home(message="Fund not selected!")

    fund = _get_fund(fund_select)
    if fund is None:
        return _redirect_home(message="Invalid fund selected")

    if not upload_instance_uuids.filename.endswith("csv"):
        return _redirect_home(message="Instance UUIDs file must be a csv")

    try:
        contents = upload_instance_uuids.file.read()
        df = pd.read_csv(BytesIO(contents), header=None)
        if df.empty:
            return _redirect_home(message="Warning! Empty Instance UUID file.")

        upload_instances_df = df.rename(columns={0: 'Instance UUID'})
        dag_runs = []
        for row in upload_instances_df.iterrows():
            instance_uuid = row[1][0]
            dag_run_id = launch_digital_bookplate_979_dag(
                instance_uuid=instance_uuid, funds=[fund]
            )
            dag_runs.append(dag_run_id)
        _save_uploaded_file(
            files_base, upload_instance_uuids.filename, upload_instances_df
        )
        launch_poll_for_979_dags_email(dag_runs=dag_runs, email=email)
        return _redirect_home(
            message=f"Triggered {len(dag_runs)} DAG run(s) for {upload_instance_uuids.filename}"
        )
    except pd.errors.EmptyDataError:
        return _redirect_home(message="Warning! Empty Instance UUID file.")
