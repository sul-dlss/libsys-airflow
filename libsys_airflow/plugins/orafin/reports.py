import logging
import pathlib
import numpy as np
import pandas as pd

from airflow.sdk.bases.operator import OperatorPartial
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from libsys_airflow.plugins.shared.utils import is_production

logger = logging.getLogger(__name__)

ap_server_options = [
    "-i /opt/airflow/vendor-keys/apserver.key",
    "-o StrictHostKeyChecking=no",
]


def get_orafin_server_info() -> str:
    if is_production():
        return "of_aplib@intxfer-prd.stanford.edu"
    return "of_aplib@intxfer-uat.stanford.edu"


def extract_rows(retrieved_csv: str) -> tuple:
    """
    Process AP csv file and returns a dictionary of updated
    """
    report_path = pathlib.Path(retrieved_csv)
    dag_run_operator = None
    with report_path.open() as fo:
        raw_report = fo.readlines()
    if len(raw_report) == 1:
        # Blank report, delete and return empty list
        report_path.unlink()
        return [], dag_run_operator
    report_df = pd.read_csv(report_path, sep="\t", dtype="object")
    if len(report_df) > 1_000:
        remaining_df = report_df.iloc[1_000:]
        report_df = report_df.iloc[0:1_000]
        remaining_path = report_path.parent / f"{report_path.stem}_01.csv"
        remaining_df.to_csv(remaining_path, sep="\t")
        dag_run_operator = TriggerDagRunOperator(
            task_id="additional-rows",
            trigger_dag_id="ap_payment_report",
            conf={"ap_report_path": str(remaining_path.absolute())},
        )
    report_rows = report_df.replace({np.nan: None}).to_dict(orient='records')
    return report_rows, dag_run_operator


def filter_files(ls_output, airflow="/opt/airflow") -> tuple:
    """
    Filters files based if they already exist in the orafin-data directory
    """
    reports = [row.strip() for row in ls_output.split(",") if row.endswith(".csv")]
    existing_reports, new_reports = [], []
    for report in reports:
        report_path = pathlib.Path(airflow) / f"orafin-files/reports/{report}"
        if report_path.exists():
            existing_reports.append({"file_name": report_path.name})
        else:
            new_reports.append({"file_name": report_path.name})
    return existing_reports, new_reports


def find_reports() -> BashOperator:
    """
    Looks for reports using ssh with the BashOperator
    """
    command = (
        ["ssh"]
        + ap_server_options
        + [
            f"{get_orafin_server_info()} "
            "ls -m /home/of_aplib/OF1_PRD/outbound/data/*.csv | tr '\n' ' '"
        ]
    )
    return BashOperator(
        task_id="find_files", bash_command=" ".join(command), do_xcom_push=True
    )


def remove_reports() -> OperatorPartial:
    """
    Removes all ap reports from the server
    """
    command = (
        ["ssh"]
        + ap_server_options
        + [
            get_orafin_server_info(),
            "rm /home/of_aplib/OF1_PRD/outbound/data/$file_name",
        ]
    )
    return BashOperator.partial(
        task_id="remove_files", bash_command=" ".join(command), do_xcom_push=True
    )


def retrieve_reports() -> OperatorPartial:
    """
    scp AP Reports from server
    """
    server_info = get_orafin_server_info()
    file_location = "/home/of_aplib/OF1_DEV/outbound/data/$file_name"
    if is_production():
        file_location = "/home/of_aplib/OF1_PRD/outbound/data/$file_name"
    command = (
        ["scp"]
        + ap_server_options
        + [
            f"{server_info}:{file_location}",
            "/opt/airflow/orafin-files/reports/",
        ]
    )
    return BashOperator.partial(task_id="scp_report", bash_command=" ".join(command))
