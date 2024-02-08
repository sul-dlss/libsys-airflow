import logging
import pathlib

from jinja2 import Template

from airflow.models import Variable
from airflow.utils.email import send_email


def generate_mapping_email(iteration_directory: str):
    """
    Generates email for mapping MARC to Instance
    """
