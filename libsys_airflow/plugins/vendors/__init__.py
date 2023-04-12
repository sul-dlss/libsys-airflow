import logging

from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)

def backup_retrieved_files(*args, **kwargs):


    logger.info(f"Backs ")
    

def check_retrieve_files(*args, **kwargs):
    context = get_current_context()
    interface_info = context.get('params')    
    # Connects
    logger.info(f"Connects to Vendor {interface_info}")


def rename_vendor_files(*args, **kwargs):
    # Renames Vendor Files
    logger.info("Rename Vendor File")


def zip_extraction(*args, **kwargs):
    # Extract
    logger.info("Zip extraction")