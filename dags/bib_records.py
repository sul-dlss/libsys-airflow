"""Imports exported MARC records from Symphony into FOLIO"""

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor


def read_marc(*args, **kwargs) -> list:
    """Stub function for reading MARC21 records"""
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
        task_id="marc21_monitor", filepath="/s/SUL/Dataload/Folio/"
    )

    monitor_file_mount.doc_md = dedent(
        """\
        ####  Monitor File Mount
        Monitor's `/s/SUL/Dataload/Folio` for new MARC21 export files"""
    )

    read_marc_records = PythonOperator(
        task_id="read_marc_records",
        python_callable=read_marc,
    )

    read_marc_records.doc_md = dedent(
        """\
        #### Reads MARC21 file
        Reads MARC21 binary file and returns a list of MARC records"""
    )

    convert_marc_to_folio = PythonOperator(
        task_id="convert_marc_to_folio",
        python_callable=convert_to_folio,
        op_kwargs={"marc_records": "{{ ti.xcom_pull('read_marc_records') }}"},
    )

    convert_marc_to_folio.doc_md = dedent(
        """\
        #### Converts MARC21 Records to validated FOLIO Inventory Records
        Task takes a list of MARC21 Records and converts them into the FOLIO
        Inventory Records"""
    )

    load_folio_records = PythonOperator(
        task_id="load_folio_records",
        python_callable=load_records,
        op_kwargs={
            "folio_records": "{{ ti.xcom_pull('convert_marc_to_folio') }}"
        },
    )

    load_folio_records.doc_md = dedent(
        """\
        #### Loads FOLIO Inventory Records
        Loads FOLIO Inventory records into a running FOLIO instance"""
    )

    monitor_file_mount >> read_marc_records >> convert_marc_to_folio
    convert_marc_to_folio >> load_folio_records
