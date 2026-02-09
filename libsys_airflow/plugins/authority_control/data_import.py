import logging

from airflow.sdk import Variable
from airflow.providers.standard.operators.bash import BashOperator

logger = logging.getLogger(__name__)


def run_folio_data_import(file_path: str, profile_name: str):
    """
    Run the folio data import
    """
    args = [
        "python3 -m folio_data_import",
        "--record-type MARC21",
        "--gateway_url $gateway_url",
        "--tenant_id sul",
        "--username $username",
        "--password $password",
        "--marc_file_path $marc_file_path",
        "--import_profile_name \"$profile_name\"",
    ]
    bash_operator = BashOperator(
        task_id="run_folio_data_import",
        bash_command=" ".join(args),
        env={
            "gateway_url": Variable.get("OKAPI_URL"),
            "username": Variable.get("FOLIO_USER"),
            "password": Variable.get("FOLIO_PASSWORD"),
            "marc_file_path": file_path,
            "profile_name": profile_name,
        },
    )
    return bash_operator
