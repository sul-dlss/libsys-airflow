import logging
import pathlib

import pandas as pd
import pymarc

from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)


def _normalize_001(field: str) -> str:
    for row in ["\\", " "]:
        field = field.replace(row, "")
    return field


def clean_csv_file(**kwargs) -> str:
    """
    Takes a csv_file of 001s, cleans 001s, and saves to new file
    and returns the new file's location
    """
    airflow_dir: str = kwargs.get("airflow", "/opt/airflow")
    csv_file: str = kwargs.get("file")

    airflow_path = pathlib.Path(airflow_dir)
    authority_uploads_path = airflow_path / "authorities/uploads"
    authority_uploads_path.mkdir(parents=True, exist_ok=True)

    csv_path = authority_uploads_path / csv_file
    if not csv_path.exists():
        raise ValueError(f"{csv_file} doesn't exist")

    csv_df = pd.read_csv(csv_path)
    csv_df["001"] = csv_df["001"].apply(_normalize_001)

    updated_csv = authority_uploads_path / f"updated-{csv_file}"
    csv_df.to_csv(updated_csv, index=False)

    return str(updated_csv.absolute())


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

    batch_size = int(Variable.get("AUTH_MAX_ENTITIES", 10_000))
    batches = []
    with open(marc21_file_path, "rb") as marc_file:
        reader = pymarc.MARCReader(marc_file)
        batch_file_name = f"{marc21_file_path.stem}_1.mrc"
        batch = pymarc.MARCWriter(open(batch_dir / batch_file_name, "wb"))
        batch_count = 1
        for i, record in enumerate(reader):
            batch.write(record)
            if not i % batch_size and i > 0:
                batch.close()
                batches.append(batch_file_name)
                batch_count += 1
                batch_file_name = f"{marc21_file_path.stem}_{batch_count}.mrc"
                batch = pymarc.MARCWriter(open(batch_dir / batch_file_name, "wb"))
        batch.close()
        batches.append(batch_file_name)

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
