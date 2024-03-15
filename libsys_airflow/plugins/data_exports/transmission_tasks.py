import logging
from pathlib import Path

from airflow.decorators import task
from airflow.models.connection import Connection
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.providers.http.hooks.http import HttpHook

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


@task(multiple_outputs=True)
def transmit_data_http_task(conn_id, local_files) -> dict:
    """
    Transmit the data via http
    Returns lists of files successfully transmitted and failures
    """
    hook = HttpHook(http_conn_id=conn_id)
    success = []
    failures = []
    for f in local_files:
        logger.info(f"Begin reading file {f} for transmission")
        with Path(f).open("rb") as fo:
            marc_data = fo.read()

        try:
            logger.info(f"Start transmission of data from file {f}")
            connection = Connection.get_connection_from_secrets(conn_id)
            hook.run(headers=connection.extra_dejson, data=marc_data)
            success.append(f)
            logger.info(f"End transmission of data from file {f}")
        except Exception as e:
            logger.error(e)
            logger.error(f"Exception for transmission of data from file {f}")
            failures.append(f)

    return {"success": success, "failures": failures}


@task(multiple_outputs=True)
def transmit_data_ftp_task(conn_id, local_files) -> dict:
    """
    Transmit the data via ftp
    Returns lists of files successfully transmitted and failures
    """
    hook = FTPHook(ftp_conn_id=conn_id)
    connection = Connection.get_connection_from_secrets(conn_id)
    remote_path = connection.extra_dejson["remote_path"]
    success = []
    failures = []
    for f in local_files:
        remote_file_path = f"{remote_path}/{Path(f).name}"
        try:
            logger.info(f"Start transmission of file {f}")
            hook.store_file(remote_file_path, f)
            success.append(f)
            logger.info(f"End transmission of file {f}")
        except Exception as e:
            logger.error(e)
            logger.error(f"Exception for transmission of file {f}")
            failures.append(f)

    return {"success": success, "failures": failures}


@task
def archive_transmitted_data_task(files):
    """
    Given a list of successfully transmitted files, move files to
    'transmitted' folder under each data-export-files/{vendor}.
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
