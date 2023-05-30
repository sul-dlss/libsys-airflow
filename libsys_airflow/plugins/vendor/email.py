import os

from airflow.configuration import conf


def email_args() -> dict:
    if not conf.has_option("smtp", "smtp_host"):
        return {}

    return {
        "email_on_failure": True,
        "email": os.getenv('VENDOR_LOADS_TO_EMAIL'),
    }
