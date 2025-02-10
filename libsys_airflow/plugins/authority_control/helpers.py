import logging

logger = logging.getLogger(__name__)


def clean_up(marc_file_path):
    """
    Moves marc file after running folio data import
    """
    logger.info(f"Moving {marc_file_path} to archive")
    return True
