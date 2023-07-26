import json
import logging
import pathlib
import shutil

import requests

from airflow.models import Variable
from airflow.operators.python import get_current_context

from folio_migration_tools.migration_tasks.migration_task_base import LevelFilter

logger = logging.getLogger(__name__)


def _save_error_record_ids(**kwargs):
    iteration_id = kwargs["iteration_id"]
    records = kwargs["records"]
    endpoint = kwargs["endpoint"]
    error_code = kwargs["error_code"]
    airflow = kwargs.get("airflow", pathlib.Path("/opt/airflow/"))

    record_base = endpoint.split("/")[1]

    error_filepath = (
        airflow
        / "migration/iterations"
        / f"{iteration_id}/results"
        / f"errors-{record_base}-{error_code}.json"
    )

    with error_filepath.open("a+") as error_file:
        for rec in records:
            error_file.write(json.dumps(rec))
            error_file.write("\n")


def archive_artifacts(*args, **kwargs):
    """Archives JSON Instances, Items, and Holdings"""
    dag = kwargs["dag_run"]

    airflow = kwargs.get("airflow", "/opt/airflow")
    tmp = kwargs.get("tmp_dir", "/tmp")

    airflow_path = pathlib.Path(airflow)
    tmp_path = pathlib.Path(tmp)

    iteration_dir = airflow_path / f"migration/iterations/{dag.run_id}"

    airflow_results = iteration_dir / "results"
    archive_directory = iteration_dir / "archive"

    if not archive_directory.exists():
        archive_directory.mkdir()

    for tmp_file in tmp_path.glob("*.json"):
        try:
            tmp_file.unlink()
        except OSError as err:
            logger.info(f"Cannot remove {tmp_file}: {err}")

    for artifact in airflow_results.glob("*.json"):
        target = archive_directory / artifact.name

        shutil.move(artifact, target)
        logger.info("Moved {artifact} to {target}")


# Determines marc_only workflow
def get_bib_files(**kwargs):
    task_instance = kwargs["task_instance"]
    context = kwargs.get("context")
    if context is None:
        context = get_current_context()
    params = context.get("params")

    bib_file_load = params.get("record_group")
    if bib_file_load is None:
        raise ValueError("Missing bib record load")
    logger.info(f"Retrieved MARC record {bib_file_load['marc']}")
    logger.info(f"Total number of associated tsv files {len(bib_file_load['tsv'])}")
    task_instance.xcom_push(key="marc-file", value=bib_file_load["marc"])
    task_instance.xcom_push(key="tsv-files", value=bib_file_load["tsv"])
    task_instance.xcom_push(key="tsv-base", value=bib_file_load["tsv-base"])
    task_instance.xcom_push(key="tsv-dates", value=bib_file_load["tsv-dates"])
    task_instance.xcom_push(key="mhld-file", value=bib_file_load.get("mhld"))
    task_instance.xcom_push(key="bwchild-file", value=bib_file_load.get("tsv-bwchild"))
    task_instance.xcom_push(
        key="tsv-instatcode", value=bib_file_load.get("tsv-instatcode")
    )
    task_instance.xcom_push(
        key="tsv-holdingsnotes", value=bib_file_load.get("tsv-holdingsnotes")
    )


def post_to_okapi(**kwargs) -> dict:
    endpoint = kwargs.get("endpoint")
    jwt = kwargs["token"]
    iteration_id = kwargs.get("iteration_id")
    records = kwargs["records"]
    payload_key = kwargs["payload_key"]

    tenant = "sul"
    okapi_url = Variable.get("okapi_url")

    okapi_instance_url = f"{okapi_url}{endpoint}"

    headers = {
        "Content-type": "application/json",
        "user-agent": "FolioAirflow",
        "x-okapi-token": jwt,
        "x-okapi-tenant": tenant,
    }

    payload = {payload_key: records} if payload_key else records

    new_record_result = requests.post(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    logger.info(
        f"Result status code {new_record_result.status_code} for {len(records)} records"  # noqa
    )

    if new_record_result.status_code > 399:
        logger.error(new_record_result.text)
        if iteration_id is None:
            kwargs["iteration_id"] = kwargs.get("dag_run").run_id  # type: ignore
        _save_error_record_ids(error_code=new_record_result.status_code, **kwargs)

    if len(new_record_result.text) < 1:
        return {}
    return new_record_result.json()


def put_to_okapi(**kwargs):
    endpoint = kwargs.get("endpoint")
    jwt = kwargs["token"]
    records = kwargs["records"]
    payload_key = kwargs["payload_key"]

    tenant = "sul"
    okapi_url = Variable.get("okapi_url")

    okapi_instance_url = f"{okapi_url}{endpoint}"

    headers = {
        "Content-type": "application/json",
        "user-agent": "FolioAirflow",
        "x-okapi-token": jwt,
        "x-okapi-tenant": tenant,
    }

    if payload_key:
        payload = {payload_key: records}
        payload["totalRecords"] = len(records)
    else:
        payload = records

    update_record_result = requests.put(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    logger.info(f"PUT Result status code {update_record_result.status_code}")  # noqa


def process_records(*args, **kwargs) -> int:
    """Function creates valid json from file of FOLIO objects"""
    prefix = kwargs.get("prefix")
    dag = kwargs["dag_run"]

    pattern = f"{prefix}*.json"

    out_filename = f"{kwargs.get('out_filename')}-{dag.run_id}"

    total_jobs = int(kwargs.get("jobs"))  # type: ignore

    airflow = kwargs.get("airflow", "/opt/airflow")
    tmp = kwargs.get("tmp", "/tmp")

    records = []
    results_dir = pathlib.Path(f"{airflow}/migration/iterations/{dag.run_id}/results")

    for file in results_dir.glob(pattern):
        logger.info(f"Loading {file}")
        with open(file) as fo:
            records.extend([json.loads(i) for i in fo.readlines()])

    shard_size = int(len(records) / total_jobs)

    for i in range(total_jobs):
        start = i * shard_size
        end = shard_size * (i + 1)
        if i == total_jobs - 1:
            end = len(records)
        tmp_out_path = f"{tmp}/{out_filename}-{i}.json"
        logger.info(f"Start {start} End {end} for {tmp_out_path}")
        with open(tmp_out_path, "w+") as fo:
            json.dump(records[start:end], fo)

    return len(records)


def setup_dag_run_folders(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]

    migration_dir = pathlib.Path(f"{airflow}/migration")
    iteration_dir = migration_dir / "iterations" / str(dag.run_id)

    logger.info("New iteration directory {iteration_dir}")

    iteration_dir.mkdir(parents=True)
    source_data = iteration_dir / "source_data"

    for folder in ["results", "reports"]:
        folder_path = iteration_dir / folder
        folder_path.mkdir(parents=True)

    for record_type in ["instances", "holdings", "items"]:
        record_path = source_data / record_type
        record_path.mkdir(parents=True)

    return str(iteration_dir)


def setup_data_logging(transformer):
    def transformer_data_issues(transformer, message, *args, **kwargs):
        transformer._log(DATA_ISSUE_LVL_NUM, message, args, **kwargs)

    # Set DATA_ISSUE logging levels
    DATA_ISSUE_LVL_NUM = 26
    logging.addLevelName(DATA_ISSUE_LVL_NUM, "DATA_ISSUES")
    logging.Logger.data_issues = transformer_data_issues

    data_issue_file_formatter = logging.Formatter("%(message)s")
    data_issue_file_handler = logging.FileHandler(
        filename=str(transformer.folder_structure.data_issue_file_path),
    )
    data_issue_file_handler.addFilter(LevelFilter(26))
    data_issue_file_handler.setFormatter(data_issue_file_formatter)
    data_issue_file_handler.setLevel(26)
    logging.getLogger().addHandler(data_issue_file_handler)
