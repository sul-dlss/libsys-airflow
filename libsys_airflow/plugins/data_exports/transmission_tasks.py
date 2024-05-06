import logging
from pathlib import Path
from s3path import S3Path
import httpx

from airflow.decorators import task
from airflow.models.connection import Connection
from airflow.providers.ftp.hooks.ftp import FTPHook

logger = logging.getLogger(__name__)


@task
def gather_files_task(**kwargs) -> dict:
    """
    Gets files to send to vendor:
    Looks for all the files in the data-export-files/{vendor}/marc-files folder
    Regardless of date stamp
    """
    logger.info("Gathering files to transmit")
    airflow = kwargs.get("airflow", "/opt/airflow")
    vendor = kwargs["vendor"]
    params = kwargs.get("params", {})
    bucket = params.get("bucket", {})
    if vendor == "full-dump":
        marc_filepath = S3Path(f"/{bucket}/data-export-files/{vendor}/marc-files/")
    else:
        marc_filepath = Path(airflow) / f"data-export-files/{vendor}/marc-files/"

    marc_filelist = []
    for f in marc_filepath.glob("**/*.mrc"):
        if f.stat().st_size == 0:
            continue
        marc_filelist.append(str(f))

    return {
        "file_list": marc_filelist,
        "s3": bool(bucket),
    }


@task(multiple_outputs=True)
def transmit_data_http_task(gather_files, **kwargs) -> dict:
    """
    Transmit the data via http
    Returns lists of files successfully transmitted and failures
    """
    success = []
    failures = []
    files_params = kwargs.get("files_params", "upload")
    url_params = kwargs.get("url_params", {})
    params = kwargs.get("params", {})
    conn_id = params["vendor"]
    logger.info(f"Transmit data to {conn_id}")
    connection = Connection.get_connection_from_secrets(conn_id)
    if gather_files["s3"]:
        path_module = S3Path
    else:
        path_module = Path
    with httpx.Client(
        headers=connection.extra_dejson, params=url_params, follow_redirects=True
    ) as client:
        for f in gather_files["file_list"]:
            files = {files_params: path_module(f).open("rb")}
            request = client.build_request("POST", connection.host, files=files)
            try:
                logger.info(f"Start transmission of data from file {f}")
                response = client.send(request)
                response.raise_for_status()
                success.append(f)
                logger.info(f"End transmission of data from file {f}")
            except httpx.HTTPError as e:
                logger.error(f"Error for {e.request.url} - {e}")
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

    archive_dir = Path(files[0]).parent.parent.parent / "transmitted"
    archive_dir.mkdir(exist_ok=True)
    for x in files:
        kind = Path(x).parent.name
        original_marc_path = Path(x)
        archive_path = archive_dir / kind / original_marc_path.name
        instance_path = (
            original_marc_path.parent.parent.parent
            / f"instanceids/{kind}/{original_marc_path.stem}.csv"
        )
        original_marc_path.replace(archive_path)

        if instance_path.exists():
            instance_path.replace(archive_dir / kind / instance_path.name)
