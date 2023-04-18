import logging
import pathlib

from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)


def backup_retrieved_files(*args, **kwargs):
    """
    Reference ticket https://github.com/sul-dlss/libsys-airflow/issues/265
    """
    task_instance = kwargs["task_instance"]
    backup_files = kwargs["backup_files"]
    zip_task_id = kwargs["zip_task_id"]
    zip_files = task_instance.xcom_pull(task_ids=zip_task_id)
    for row in backup_files + zip_files:
        logger.info(f"Backing up {row}")


def check_retrieve_files(*args, **kwargs):
    context = get_current_context()
    interface_info = context.get('params')
    task_instance = kwargs["task_instance"]

    # Connects
    logger.info(f"Connects to Vendor {interface_info}")
    interface_path = "/opt/airflow/dataloader/gobi/2023-04-13"
    task_instance.xcom_push(key="marc-brief-orders", value=f"{interface_path}/sample.mrc")
    task_instance.xcom_push(key="zipfile", value=f"{interface_path}/sample.zip")
    task_instance.xcom_push(key="full-marc", value=f"{interface_path}/full-sample.mrc")
    task_instance.xcom_push(key="invoice", value=f"{interface_path}/invoice.edi")


def rename_vendor_files(*args, **kwargs):
    """
    Reference ticket https://github.com/sul-dlss/libsys-airflow/issues/263
    """
    # Renames Vendor Files
    files = kwargs["files"]
    task_instance = kwargs["task_instance"]
    extracted_zipfiles = task_instance.xcom_pull(task_ids='extract-zipfiles')
    files = files + extracted_zipfiles
    renamed_files = []
    for i, file in enumerate(files):
        logger.info(f"Renaming {file}")
        renamed_files.append(f"{file}{i}")
    task_instance.xcom_push(key="files", value=renamed_files)

    logger.info("Finished Renaming Vendor File(s)")


def zip_extraction(*args, **kwargs):
    """
    Reference ticket https://github.com/sul-dlss/libsys-airflow/issues/262
    """
    # Extract
    zipfile = kwargs["zipfile"]
    path = pathlib.Path(zipfile)
    logger.info(f"Zip extraction for {zipfile}")
    # Returning value from a task's function is put into the default XCOM
    return [f"{path.parent}/sample-01.mrc", f"{path.parent}/sample-02.mrc"]
