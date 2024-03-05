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


@task
def transmit_data_task(connection_details):
    """
    Transmit the data
    """
    logger.info("Transmitting the data")


@task
def archive_transmitted_data_task(**kwargs):
    """
    Looks at date-stamp filename once we know that the transmission was a success
    Then also move the instanceid file with the same vendor and filename
    Make a 'transmitted' folder under each data-export-files/{vendor}.
    """
    logger.info("Moving transmitted files to archive directory")
