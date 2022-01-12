"""Imports exported MARC records from Symphony into FOLIO"""

from datetime import datetime, timedelta
from textwrap import dedent
from typing_extensions import TypeAlias

from airflow import DAG

from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

from folio_post import post_folio_instance_records

def read_marc(*args, **kwargs) -> list:
    """Stub function for reading MARC21 records"""
    print("in read_marc")
    return []


def convert_to_folio(*args, **kwargs) -> list:
    """Stub function for converting list of MARC21 to FOLIO json records"""
    return []


def load_records(*args, **kwargs) -> bool:
    """Stub function for loading Inventory records into FOLIO"""
    return True


default_args = {
    "owner": "folio",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "symphony_marc_import",
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 1, 3),
    catchup=False,
    tags=["bib_import"],
) as dag:

    dag.doc_md = dedent(
        """
    # Import Symphony MARC Records to FOLIO
    Workflow for monitoring a file mount of exported MARC21 records from
    Symphony ILS into [FOLIO](https://www.folio.org/) LSM.
    """
    )

    monitor_file_mount = FileSensor(
        task_id="marc21_monitor",
        fs_conn_id="bib_path",
        filepath="/opt/airflow/symphony/*.*rc",
        timeout=60 * 30
    )

    monitor_file_mount.doc_md = dedent(
        """\
        ####  Monitor File Mount
        Monitor's `/s/SUL/Dataload/Folio` for new MARC21 export files"""
    )

    copy_marc_instance_files = BashOperator(
        task_id="move_marc_file",
        bash_command="for F in /opt/airflow/symphony/*.marc; do if [ ! -f /opt/airflow/migration/archive/$(basename $F) ];then cp $F /opt/airflow/migration/data/instance/; fi; done"
    )

    convert_marc_to_folio = BashOperator(
        task_id="convert_marc_to_folio",
        bash_command="python /opt/airflow/MARC21-To-FOLIO/main_bibs.py --password $password --ils_flavour $ils_flavor --folio_version $folio_version --holdings_records False --force_utf_8 False --dates_from_marc False --hrid_handling False --suppress False /opt/airflow/migration $okapi_url $tenant $user",
        env={ "folio_version": "iris",
            "ils_flavor": "001",
            "okapi_url": Variable.get("OKAPI_URL"),
            "password": Variable.get("FOLIO_PASSWORD"),
            "tenant": "sul",
            "user": Variable.get("FOLIO_USER")
        }
    )

    post_to_folio = PythonOperator(
        task_id="post_to_folio_instances",
        python_callable=post_folio_instance_records
    )

    archive_marc_instance_files = BashOperator(
        task_id="archive_coverted_files",
        bash_command="mv /opt/airflow/migration/data/instance/* /opt/airflow/migration/archive/."
    )

    convert_marc_to_folio.doc_md = dedent(
        """\
        #### Converts MARC21 Records to validated FOLIO Inventory Records
        Task takes a list of MARC21 Records and converts them into the FOLIO
        Inventory Records"""
    )

    finish_loading = DummyOperator(
        task_id="finish_loading",
    )

    monitor_file_mount >> copy_marc_instance_files
    copy_marc_instance_files >> convert_marc_to_folio >> post_to_folio
    post_to_folio >>archive_marc_instance_files >> finish_loading
