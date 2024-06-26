import logging
from pathlib import Path
from s3path import S3Path
import httpx

from airflow.decorators import task
from airflow.models.connection import Connection
from airflow.providers.ftp.hooks.ftp import FTPHook

from libsys_airflow.plugins.data_exports.oclc_api import OCLCAPIWrapper
from libsys_airflow.plugins.shared.utils import is_production

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


@task
def retry_failed_files_task(**kwargs) -> dict:
    """
    Returns a list of files and s3 boolean
    Uses the list of failed files from xcom
    """
    marc_filelist = []
    params = kwargs.get("params", {})
    bucket = params.get("bucket", {})
    if len(kwargs["files"]) == 0:
        logger.info("No failures to retry")
    else:
        logger.info("Retry failed files")
        marc_filelist = kwargs["files"]

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
    for marc_file_path in oclc_directory.glob("**/*.mrc"):
        type_of = marc_file_path.parent.name
        library = marc_file_path.stem.split("-")[1]
        if type_of in libraries[library]:
            libraries[library][type_of].append(str(marc_file_path))
        else:
            libraries[library][type_of] = [str(marc_file_path)]
    return libraries


@task
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


@task
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
        remote_file_name = vendor_filename_spec(conn_id, f)
        remote_file_path = f"{remote_path}/{remote_file_name}"
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
    success: dict = {}
    failures: dict = {}
    archive: list = []

    connection_lookup = oclc_connections(connection_details)

    for library, records in libraries.items():
        success[library] = []
        failures[library] = []

        oclc_api = OCLCAPIWrapper(
            client_id=connection_lookup[library]["username"],
            secret=connection_lookup[library]["password"],
        )
        if len(records.get("deletes", [])) > 0:
            delete_result = oclc_api.delete(records['deletes'])
            success[library].extend(delete_result['success'])
            failures[library].extend(delete_result['failures'])
            archive.extend(delete_result['archive'])

        if len(records.get("new", [])) > 0:
            new_result = oclc_api.new(records['new'])
            success[library].extend(new_result['success'])
            failures[library].extend(new_result['failures'])
            archive.extend(new_result['archive'])

        if len(records.get("updates", [])) > 0:
            updated_result = oclc_api.update(records['updates'])
            success[library].extend(updated_result['success'])
            failures[library].extend(updated_result['failures'])
            archive.extend(updated_result['archive'])

    archive = list(set(archive))
    return {"success": success, "failures": failures, "archive": archive}


@task
def archive_transmitted_data_task(files):
    """
    Given a list of successfully transmitted files, move files to
    'transmitted' folder under each data-export-files/{vendor}.
    Also moves the instanceid file with the same vendor and filename
    Also moves the marc files with the same filename as what was transmitted (i.e. GOBI txt files)
    """
    logger.info("Moving transmitted files to archive directory")
    if len(files) < 1:
        logger.warning("No files to archive")
        return

    archive_dir = Path(files[0]).parent.parent.parent / "transmitted"
    archive_dir.mkdir(exist_ok=True)
    for x in files:
        kind = Path(x).parent.name
        # original_transmitted_file_path = data-export-files/{vendor}/marc-files/new|updates|deletes/*.mrc|*.txt
        original_transmitted_file_path = Path(x)

        # archive_path = data-export-files/{vendor}/transmitted/new|updates|deletes
        archive_path = archive_dir / kind
        archive_path.mkdir(exist_ok=True)
        archive_path = archive_path / original_transmitted_file_path.name

        # instance_path = data-export-files/{vendor}/instanceids/new|updates|deletes/*.csv
        instance_path = (
            original_transmitted_file_path.parent.parent.parent
            / f"instanceids/{kind}/{original_transmitted_file_path.stem}.csv"
        )
        instance_archive_path = archive_dir / kind / instance_path.name

        marc_path = (
            original_transmitted_file_path.parent
            / f"{original_transmitted_file_path.stem}.mrc"
        )
        marc_archive_path = archive_dir / kind / marc_path.name

        # move transmitted files (for GOBI this will be *.txt files)
        logger.info(
            f"Moving transmitted file {original_transmitted_file_path} to {archive_path}"
        )
        original_transmitted_file_path.replace(archive_path)

        # move instance id files with same stem as transmitted filename
        if instance_path.exists():
            logger.info(
                f"Moving related instanceid file {instance_path} to {instance_archive_path}"
            )
            instance_path.replace(instance_archive_path)

        # move marc files with same stem as transmitted filename (when transmitted file is not *.mrc)
        if marc_path.exists():
            logger.info(f"Moving related marc file {marc_path} to {marc_archive_path}")
            marc_path.replace(marc_archive_path)


def vendor_filename_spec(conn_id, filename):
    """
    Returns a filename per the vendor's filenaming convention
    """
    if conn_id == "gobi":
        # gobi should have "stf" prepended
        return "stf" + Path(filename).name
    elif conn_id == "sharevde":
        return "tbd"
    else:
        Path(filename).name


def return_success_test_instance(files) -> dict:
    logger.info("SKIPPING TRANSMISSION")
    return {"success": files["file_list"], "failures": []}


def oclc_connections(connection_details: list) -> dict:
    connection_lookup = {}
    for conn_id in connection_details:
        connection = Connection.get_connection_from_secrets(conn_id)
        oclc_code = connection.extra_dejson["oclc_code"]
        connection_lookup[oclc_code] = {
            "username": connection.login,
            "password": connection.password,
        }
    return connection_lookup
