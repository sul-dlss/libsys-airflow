import logging
from pathlib import Path

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
def gather_files_task(**kwargs) -> list:
    """
    Gets files to send to vendor:
    Looks for all the files in the data-export-files/{vendor}/marc-files folder
    Regardless of date stamp
    """
    logger.info("Gathering files to transmit")
    airflow = kwargs.get("airflow", "/opt/airflow")
    vendor = kwargs["vendor"]
    marc_filepath = Path(airflow) / f"data-export-files/{vendor}/marc-files/"
    return [str(p) for p in marc_filepath.glob("*.mrc")]


@task
def connection_details_task(**kwargs):
    """
    Given vendor name get connection details to transmit data
    """
    logger.info("Connection details for vendor")


@task(multiple_outputs=True)
def transmit_data_task(connection_details) -> dict:
    """
    Transmit the data
    Returns lists of files successfully transmitted and failures
    """
    logger.info("Transmitting the data")
    return {"success": [], "failures": []}


@task
def archive_transmitted_data_task(files):
    """
    Give list of successfully transmitted files, move to
    'transmitted' folder under each data-export-files/{vendor}
    Also moves the instanceid file with the same vendor and filename
    """
    logger.info("Moving transmitted files to archive directory")
    if len(files) < 1:
        logger.warning("No files to archive")
        return

    archive_dir = Path(files[0]).parent.parent / "transmitted"
    archive_dir.mkdir(exist_ok=True)
    for x in files:
        original_marc_path = Path(x)
        archive_path = archive_dir / original_marc_path.name
        instance_path = (
            original_marc_path.parent.parent
            / f"instanceids/{original_marc_path.stem}.csv"
        )
        if instance_path.exists():
            instance_path.replace(archive_dir / instance_path.name)
        original_marc_path.replace(archive_path)
