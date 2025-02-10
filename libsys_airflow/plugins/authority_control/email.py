import logging

logger = logging.getLogger(__name__)


def email_report(bash_result: str):
    logger.info(f"Emailing load report: {bash_result}")
