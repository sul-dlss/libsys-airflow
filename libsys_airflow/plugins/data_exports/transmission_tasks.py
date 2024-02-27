import logging

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
def gather_files_task(**kwargs):
    """
    Gets files to send to vendor
    """
    logger.info("Gathering files to transmit")


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
