"""Imports exported MARC records from Symphony into FOLIO"""

from datetime import datetime, timedelta
import pathlib
import json
import logging
from textwrap import dedent

from airflow import DAG

from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

from folio_post import post_folio_instance_records, post_folio_holding_records

logger = logging.getLogger(__name__)

def process_records(*args, **kwargs) -> list:
    """Function creates valid json from file of FOLIO objects"""
    pattern = kwargs.get("pattern")
    out_filename = kwargs.get("out_filename")
    records = []
    for file in pathlib.Path("/opt/airflow/migration/results").glob(pattern):
        with open(file) as fo:
            records.extend([json.loads(i) for i in fo.readlines()])

    logger.info(f"{out_filename} contains {len(records)} number of records")
    with open(f"/tmp/{out_filename}", "w+") as fo:
        json.dump(records, fo)


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
        timeout=60 * 30,
    )

    monitor_file_mount.doc_md = dedent(
        """\
        ####  Monitor File Mount
        Monitor's `/s/SUL/Dataload/Folio` for new MARC21 export files"""
    )

    copy_marc_instance_files = BashOperator(
        task_id="move_marc_file",
        bash_command="mv /opt/airflow/symphony/*.marc /opt/airflow/migration/data/instance/",
    )

    with TaskGroup(group_id="marc-to-folio") as marc_to_folio:

        convert_marc_to_folio = BashOperator(
            task_id="convert_marc_to_folio",
            bash_command="python /opt/airflow/MARC21-To-FOLIO/main_bibs.py --password $password --ils_flavour $ils_flavor --folio_version $folio_version --holdings_records False --force_utf_8 False --dates_from_marc False --hrid_handling default --suppress False /opt/airflow/migration $okapi_url $tenant $user",
            env={
                "folio_version": "iris",
                "ils_flavor": "001",
                "okapi_url": Variable.get("OKAPI_URL"),
                "password": Variable.get("FOLIO_PASSWORD"),
                "tenant": "sul",
                "user": Variable.get("FOLIO_USER"),
            },
        )

        convert_marc_to_folio.doc_md = dedent(
            """\
        #### Converts MARC21 Records to validated FOLIO Inventory Records
        Task takes a list of MARC21 Records and converts them into the FOLIO
        Inventory Records"""
        )

        convert_instances_valid_json = PythonOperator(
            task_id="instances_to_valid_json",
            python_callable=process_records,
            op_kwargs={
                "pattern": "folio_instance_*.json",
                "out_filename": "instances.json",
            },
        )

        convert_holdings_valid_json = PythonOperator(
            task_id="holdings_to_valid_json",
            python_callable=process_records,
            op_kwargs={
                "pattern": "folio_holdingsrecord_*.json",
                "out_filename": "holdings.json",
            },
        )

        finish_conversion = DummyOperator(task_id="finish_json_conversions")

        (
            convert_marc_to_folio
            >> [convert_instances_valid_json, convert_holdings_valid_json]
            >> finish_conversion
        )

    with TaskGroup(group_id="post-to-folio") as post_to_folio:
        post_instances_to_folio = PythonOperator(
            task_id="post_to_folio_instances",
            python_callable=post_folio_instance_records,
        )

        post_holdings_to_folio = PythonOperator(
            task_id="post_to_folio_holdings", python_callable=post_folio_holding_records
        )

        post_instances_to_folio >> post_holdings_to_folio

    archive_files = BashOperator(
        task_id="archive_coverted_files",
        bash_command="mv /opt/airflow/migration/data/instance/* /opt/airflow/migration/archive/.; mv /opt/airflow/migration/results/folio_instance_*.json /opt/airflow/migration/archive/.; mv /opt/airflow/migration/results/folio_holdingsrecord_*.json /opt/airflow/migration/archive/.",
    )

    finish_loading = DummyOperator(
        task_id="finish_loading",
    )

    monitor_file_mount >> copy_marc_instance_files >> marc_to_folio
    marc_to_folio >> post_to_folio >> archive_files >> finish_loading
