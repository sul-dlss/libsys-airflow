import pathlib

from airflow.decorators import task

from libsys_airflow.plugins.vendor.marc_filter import filter_fields


@task
def filter_fields_task(marc_path: str, fields: list):
    """
    Filters 905, 920, 986 from MARC records
    """
    filter_fields(pathlib.Path(marc_path), ['905', '920', '986'])
