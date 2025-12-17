import datetime
import logging
import pathlib

import pandas as pd
import pymarc

from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from libsys_airflow.plugins.shared.folio_client import folio_client

logger = logging.getLogger(__name__)


def _normalize_001(field: str) -> str:
    for row in ["\\", " "]:
        field = field.replace(row, "")
    return field


def archive_csv_files(**kwargs):
    """
    Archives CSV files of 001s
    """
    csv_files = kwargs.get("csv_files", [])
    airflow = kwargs.get("airflow", "/opt/airflow")
    airflow_path = pathlib.Path(airflow)

    archive_path = airflow_path / "authorities/archive/uploads"
    archive_path.mkdir(parents=True, exist_ok=True)
    for csv_file in csv_files:
        csv_file_path = pathlib.Path(csv_file)
        if not csv_file_path.exists():
            logger.error(f"{csv_file} does not exist, cannot archive")
            continue
        csv_archive_path = archive_path / csv_file_path.name
        csv_file_path.rename(csv_archive_path)
        logger.info(f"Archived {csv_file_path.name} to {csv_archive_path}")


def batch_csv(**kwargs) -> list:
    """
    Takes a csv_file of 001s and generates a list of batch files
    """
    csv_absolute_file: str = kwargs["file"]
    batch_size = int(Variable.get("AUTH_MAX_ENTITIES", 500))
    csv_path = pathlib.Path(csv_absolute_file)

    uploads_path = csv_path.parent

    csv_df = pd.read_csv(csv_path)
    batches_paths = []
    count = 1
    for i in range(0, len(csv_df), batch_size):
        batch_001s = csv_df.iloc[i : i + batch_size]
        batch_path = uploads_path / f"{csv_path.stem}-{count:02d}.csv"
        batch_001s.to_csv(batch_path, index=False)
        batches_paths.append(str(batch_path.absolute()))
        count += 1
    return batches_paths


def clean_csv_file(**kwargs) -> str:
    """
    Takes a csv_file of 001s, cleans 001s, and saves to new file
    and returns the new file's location
    """
    airflow_dir: str = kwargs.get("airflow", "/opt/airflow")
    csv_file: str = kwargs["file"]

    airflow_path = pathlib.Path(airflow_dir)
    authority_uploads_path = airflow_path / "authorities/uploads"
    authority_uploads_path.mkdir(parents=True, exist_ok=True)

    csv_path = authority_uploads_path / csv_file
    if not csv_path.exists():
        raise ValueError(f"{csv_file} doesn't exist")

    csv_df = pd.read_csv(csv_path)
    csv_df["001"] = csv_df["001"].apply(_normalize_001)
    timestamp = datetime.datetime.now(datetime.UTC)
    updated_csv = (
        authority_uploads_path / f"updated-{int(timestamp.timestamp())}-{csv_file}"
    )
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
    Creates 1 or more batch files
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


def delete_authorities(**kwargs) -> dict:
    """
    Deletes FOLIO authority record by UUID, returns count of total deleted and
    any errors
    """
    delete_uuids = kwargs.get("deletes", [])
    client = folio_client(**kwargs)
    output: dict = {"deleted": 0, "errors": []}
    logger.info(f"Starting deletion of {len(delete_uuids):,}")
    for uuid in delete_uuids:
        try:
            client.folio_delete(f"/authority-storage/authorities/{uuid}")
            output["deleted"] += 1
        except Exception as e:
            output['errors'].append(f"{uuid}: {e}")
            logger.error(output['errors'][-1])
    logger.info(
        f"Deleted: {output['deleted']:,}, total errors: {len(output['errors'])}"
    )
    return output


def find_authority_by_001(**kwargs) -> dict:
    """
    Searches authority FOLIO records by 001 via naturalid property. Tracks if
    search returns more than one matching record or if no records are found.
    """
    output: dict = {"deletes": [], "errors": [], "missing": [], "multiples": []}
    csv_file = kwargs.get("file")
    client = folio_client(**kwargs)
    csv_df = pd.read_csv(csv_file)
    logger.info(f"Finding FOLIO authority records for {len(csv_df):,}")
    for field001 in csv_df["001s"]:
        try:
            auth_records = client.folio_get(
                "/authority-storage/authorities",
                key="authorities",
                query=f"naturalId=={field001}",
            )
            match len(auth_records):

                case 0:
                    output["missing"].append(field001)

                case 1:
                    output["deletes"].append(auth_records[0]['id'])

                case _:
                    multiple_auth_uuids = ",".join([r['id'] for r in auth_records])
                    output["multiples"].append(f"{field001}: {multiple_auth_uuids}")
        except Exception as e:
            output["errors"].append(f"{field001}: {e}")
    logger.info(
        f"{len(output['deletes']):,} deletes, {len(output['missing']):,} missing, {len(output['errors']):,} errors, {len(output['multiples']):,} multiples"
    )
    return output


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
