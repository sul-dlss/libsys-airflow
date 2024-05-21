import logging
from pathlib import Path
from s3path import S3Path
import httpx

from airflow.decorators import task
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.providers.ftp.hooks.ftp import FTPHook

from libsys_airflow.plugins.data_exports.oclc_api import OCLCAPIWrapper

logger = logging.getLogger(__name__)


@task
def gather_files_task(**kwargs) -> dict:
    """
    Gets files to send to vendor:
    Looks for all the files in the data-export-files/{vendor}/marc-files folder
    File glob patterns include "**/" to get the deletes, new, and updates folders
    Regardless of date stamp
    """
    logger.info("Gathering files to transmit")
    airflow = kwargs.get("airflow", "/opt/airflow")
    vendor = kwargs["vendor"]
    params = kwargs.get("params", {})
    bucket = params.get("bucket", {})
    marc_filepath = Path(airflow) / f"data-export-files/{vendor}/marc-files/"
    file_glob_pattern = "**/*.mrc"
    if vendor == "full-dump":
        marc_filepath = S3Path(f"/{bucket}/data-export-files/{vendor}/marc-files/")
    if vendor == "gobi":
        file_glob_pattern = "**/*.txt"
    marc_filelist = []
    for f in marc_filepath.glob(file_glob_pattern):
        if f.stat().st_size == 0:
            continue
        marc_filelist.append(str(f))

    return {
        "file_list": marc_filelist,
        "s3": bool(bucket),
    }


@task(multiple_outputs=True)
def gather_oclc_files_task(**kwargs) -> dict:
    """
    Gets new and updated MARC files by library (SUL, Business, Hoover, and Law)
    to send to OCLC
    """
    airflow = kwargs.get("airflow", "/opt/airflow")
    libraries: dict = {
        "S7Z": {},  # Business
        "HIN": {},  # Hoover
        "CASUM": {},  # Lane
        "RCJ": {},  # Law
        "STF": {},  # SUL
    }
    oclc_directory = Path(airflow) / "data-export-files/oclc/marc-files/"
    for marc_file_path in oclc_directory.glob("*.mrc"):
        file_parts = marc_file_path.name.split("-")
        if len(file_parts) < 3:
            continue
        library, type_of = file_parts[1], file_parts[2].split(".")[0]
        if type_of in libraries[library]:
            libraries[library][type_of].append(str(marc_file_path))
        else:
            libraries[library][type_of] = [str(marc_file_path)]
    return libraries


@task(multiple_outputs=True)
def transmit_data_http_task(gather_files, **kwargs) -> dict:
    if not is_production():
        return return_success_test_instance(gather_files)
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
def transmit_data_ftp_task(conn_id, gather_files) -> dict:
    if not is_production():
        return return_success_test_instance(gather_files)
    """
    Transmit the data via ftp
    Returns lists of files successfully transmitted and failures
    """
    hook = FTPHook(ftp_conn_id=conn_id)
    connection = Connection.get_connection_from_secrets(conn_id)
    remote_path = connection.extra_dejson["remote_path"]
    success = []
    failures = []
    for f in gather_files["file_list"]:
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
def transmit_data_oclc_api_task(connection_details, libraries) -> dict:
    connection_lookup, success, failures = {}, {}, {}
    for conn_id in connection_details:
        connection = Connection.get_connection_from_secrets(conn_id)
        oclc_code = connection.extra_dejson["oclc_code"]
        connection_lookup[oclc_code] = {
            "username": connection.login,
            "password": connection.password,
        }

    for library, records in libraries.items():
        oclc_api = OCLCAPIWrapper(
            client_id=connection_lookup[library]["username"],
            secret=connection_lookup[library]["password"],
        )
        if len(records.get("new", [])) > 0:
            new_result = oclc_api.new(records['new'])
            success[library] = new_result['success']
            failures[library] = new_result['failures']

        if len(records.get("update", [])) > 0:
            updated_result = oclc_api.update(records['update'])
            success[library].extend(updated_result['success'])
            failures[library].extend(updated_result['failures'])

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
        archive_path = archive_dir / kind
        archive_path.mkdir(exist_ok=True)
        archive_path = archive_path / original_marc_path.name
        instance_path = (
            original_marc_path.parent.parent.parent
            / f"instanceids/{kind}/{original_marc_path.stem}.csv"
        )
        original_marc_path.replace(archive_path)

        if instance_path.exists():
            instance_path.replace(archive_dir / kind / instance_path.name)


def is_production():
    return bool(Variable.get("OKAPI_URL").find("prod") > 0)


def return_success_test_instance(files) -> dict:
    logger.info("SKIPPING TRANSMISSION")
    return {"success": files["file_list"], "failures": []}
