import logging
import pathlib

import pymarc

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)


def clean_up(marc_file: str, airflow: str = '/opt/airflow') -> bool:
    """
    Moves marc file after running folio data import
    """
    marc_file_path = pathlib.Path(marc_file)
    archive_dir = pathlib.Path(airflow) / "authorities/archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    archive_file = archive_dir / marc_file_path.name
    marc_file_path.rename(archive_file)

    logger.info(f"Moved {marc_file_path} to archive")
    return True


def create_batches(marc21_file: str, airflow: str = '/opt/airflow/') -> list:
    """
    Creates 1 or more 50k batch files
    """
    marc21_file_path = pathlib.Path(marc21_file)
    batch_dir = pathlib.Path(airflow) / "authorities"
    batch_dir.mkdir(parents=True, exist_ok=True)

    batches = []
    with open(marc21_file_path, "rb") as marc_file:
        reader = pymarc.MARCReader(marc_file)
        batch = pymarc.MARCWriter(open(batch_dir / "batch_1.mrc", "wb"))
        batch_count = 1
        for i, record in enumerate(reader):
            batch.write(record)
            if not i % 50_000 and i > 0:
                batch.close()
                batches.append(f"batch_{batch_count}.mrc")
                batch_count += 1
                batch = pymarc.MARCWriter(
                    open(batch_dir / f"batch_{batch_count}.mrc", "wb")
                )
        batch.close()
        batches.append(f"batch_{batch_count}.mrc")

    logger.info(f"Created {len(batches)} batches from {marc21_file_path}")
    return batches


def trigger_load_record_dag(file_path: str, profile_name: str) -> TriggerDagRunOperator:
    """
    Triggers load_marc_file DAG with file path and profile name
    """
    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_load_record_dag",
        trigger_dag_id="load_marc_file",
        conf={"kwargs": {"file": file_path, "profile": profile_name}},
    )
    logger.info(f"Triggered load_marc_file DAG with {file_path} and {profile_name}")
    return trigger_dag
