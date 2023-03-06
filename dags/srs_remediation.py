import json
import logging
import pathlib
import tarfile
import uuid

import requests

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from folioclient import FolioClient

logger = logging.getLogger(__name__)

FOLIO_CLIENT = FolioClient(
    Variable.get("OKAPI_URL"),
    "sul",
    Variable.get("FOLIO_USER"),
    Variable.get("FOLIO_PASSWORD"),
)

def _check_add_srs_records(srs_record, snapshot_id):
    srs_id = srs_record["id"]
    check_record = requests.get(f"{FOLIO_CLIENT.okapi_url}/source-storage/records/{srs_id}",
                                headers=FOLIO_CLIENT.okapi_headers)
    match check_record.status_code:

        case 200:
            return
        
        case 404:
            srs_record['snapshotId'] = snapshot_id
            add_result = requests.post(f"{FOLIO_CLIENT.okapi_url}/source-storage/records",
                                       headers=FOLIO_CLIENT.okapi_headers)
            if add_result.status_code != 201:
                logger.error(f"Failed to add {srs_id} http-code {add_result.status_code} error {add_result.text}")

        case _:
            logger.error(f"Failed to retrieve {srs_id}, {check_record.status_code} message {check_record.text}")


def _extract_check_add_srs(*args, **kwargs):
    tarfile_path = kwargs["tarfile"]
    snapshot_id = kwargs["snapshot_id"]
    job_tarfile = tarfile.open(tarfile_path)
    
    srs_records = 0
    for name in job_tarfile.getnames():
        if "folio_srs" in name:
            logger.info(f"Extracting SRS records from {name}")
            srs_file = job_tarfile.extractfile(name)
            for line in srs_file.readlines():
                _check_add_srs_records(json.loads(line), snapshot_id)
                if not srs_records%1_000:
                    logger.info(f"Checked/Added {srs_records:,} SRS records")
                srs_records += 1
    logger.info(f"Finished extracting {srs_records:,}")


def _get_snapshot_id():
    snapshot_id = str(uuid.uuid4())
    snapshot_result = requests.post(
        f"{FOLIO_CLIENT.okapi_url}/source-storage/snapshots",
        headers=FOLIO_CLIENT.okapi_headers,
        json={
            "jobExecutionId": snapshot_id,
            "status": "PARSING_IN_PROGRESS"
        }
    )
    if snapshot_result.status_code != 201:
        logger.error(f"Error getting snapshot {snapshot_result.text}")
    return snapshot_id


def handle_srs_tarfiles(*args, **kwargs):
    task_instance = kwargs["task_instance"]
    job_id = kwargs["job"]

    tar_files = task_instance.xcom_pull(task_ids="find-tar-files", key=f"job-{job_id}")
    total_files = len(tar_files)
    snapshot_id = _get_snapshot_id()
    logger.info(f"Using snapshot id {snapshot_id}")
    logger.info(f"Stared processing SRS {total_files:,} tarfiles")
    for row in tar_files:
        logger.info(f"Processing {row}")
        _extract_check_add_srs(tarfile=row, snapshot_id=snapshot_id)
    logger.info(f"Finished processing {total_files:,}")


def batch_tarfiles(*args, **kwargs):
    task_instance = kwargs["task_instance"]
    airflow = kwargs.get("airflow", "/opt/airflow")
    job_bkups = pathlib.Path(airflow) / "migration/job_bkups"
    tar_files = []
    for row in job_bkups.glob("*.tar.gz"):
        tar_files.append(str(row))
    logger.info(f"Added {len(tar_files)}")

    shard_size = int(len(tar_files) / 5)

    for i in range(5):
        start = i * shard_size
        end = shard_size * (i + 1)
        if i == 4:
            end = len(tar_files)
        task_instance.xcom_push(key=f"job-{i}", value=tar_files[start:end])
    
    logger.info(f"Finished tar file discovery")


with DAG(
    "srs_audit_checks",
    default_args={
        "owner": "folio",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2023, 3, 2),
    catchup=False,
    tags=["bib_import", "folio"],
    max_active_runs=1,
) as dag:
    
    find_tarfiles = PythonOperator(
        task_id="find-tar-files",
        python_callable=batch_tarfiles
    )

    start_srs_check_add = EmptyOperator(task_id="start-srs-check-add")

    finished_srs_check_add = EmptyOperator(task_id="end-srs-check-add")


    for i in range(5):
        extract_checks_adds = PythonOperator(
            task_id=f"extract-check-add-{i}",
            python_callable=handle_srs_tarfiles,
            op_kwargs={ "job": i }
        )

        start_srs_check_add >> extract_checks_adds >> finished_srs_check_add

    find_tarfiles >> start_srs_check_add