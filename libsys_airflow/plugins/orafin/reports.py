import logging
import pathlib

from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

ap_server_options = [
    "-i /opt/airflow/vendor-keys/apdrop.key",
    "-o KexAlgorithms=diffie-hellman-group14-sha1",
    "-o StrictHostKeyChecking=no",
]


def filter_files(ls_output, airflow="/opt/airflow") -> tuple:
    """
    Filters files based if they already exist in the orafin-
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


def find_reports():
    """
    Looks for reports using ssh with the BashOperator
    """
    command = (
        ["ssh"]
        + ap_server_options
        + [
            "of_aplib@extxfer.stanford.edu "
            "ls -m /home/of_aplib/OF1_PRD/outbound/data/*.csv"
        ]
    )
    return BashOperator(
        task_id="find_files", bash_command=" ".join(command), do_xcom_push=True
    )


def remove_reports():
    """
    Removes all ap reports from the server
    """
    command = (
        ["ssh"]
        + ap_server_options
        + [
            "of_aplib@extxfer.stanford.edu",
            "rm /home/of_aplib/OF1_PRD/outbound/data/$file_name",
        ]
    )
    return BashOperator.partial(
        task_id="remove_files", bash_command=" ".join(command), do_xcom_push=True
    )


def retrieve_reports():
    """
    scp AP Reports from server
    """
    command = (
        ["scp"]
        + ap_server_options
        + [
            "of_aplib@extxfer.stanford.edu:/home/of_aplib/OF1_PRD/outbound/data/$file_name",
            "/opt/airflow/orafin-files/reports/",
        ]
    )
    return BashOperator.partial(task_id="scp_report", bash_command=" ".join(command))
