"""Manages SDR related tags for Items"""

import logging

from datetime import datetime

import folioclient

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule


from libsys_airflow.plugins.sdr.helpers import (
    check_update_item,
    concat_missing_barcodes,
    delete_barcode_csv,
    extract_barcodes,
    save_missing_barcodes,
    stat_codes_lookup,
)

logger = logging.getLogger(__name__)

# Suppressing folio client logging of all httpx requests
logging.getLogger('folioclient').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)


def _folio_client():
    return folioclient.FolioClient(
        Variable.get("OKAPI_URL"),
        "sul",
        Variable.get("FOLIO_USER"),
        Variable.get("FOLIO_PASSWORD"),
    )


@dag(
    schedule=None,
    start_date=datetime(2025, 11, 5),
    catchup=False,
    tags=["sdr", "folio"],
    max_active_runs=1,
)
def manage_sdr_stat_codes(**kwargs):
    """Manage Statistical Codes for SDR Items"""

    @task.branch(task_id="setup_stat_code_management")
    def setup_dag(**kwargs):
        """Task checks for incoming csv containing barcodes routes to SDR API pull if not present"""
        task_instance = kwargs["ti"]
        context = get_current_context()
        params = context.get("params", {})
        csv_file = params["kwargs"].get("file")
        if csv_file is None:
            return "sdr_pull_items"
        task_instance.xcom_push(key="csv_file", value=csv_file)
        return "get_batches_from_csv"

    @task
    def get_batches_from_csv(**kwargs):
        """Retrieves and batches barcodes from csv file"""
        task_instance = kwargs["ti"]
        csv_file = task_instance.xcom_pull(
            task_ids="setup_stat_code_management", key="csv_file"
        )
        barcode_batches = extract_barcodes(csv_file)
        return barcode_batches

    @task(task_id="stat_code_lookup")
    def get_stat_code_lookup(**kwargs):
        return stat_codes_lookup(_folio_client())

    @task(max_active_tis_per_dag=3)
    def process_barcode_batch(**kwargs):
        barcode_batch = kwargs["batch"]
        task_instance = kwargs["ti"]
        stat_code_lookup = task_instance.xcom_pull(task_ids="stat_code_lookup")
        folio_client = _folio_client()
        total_records, errors = len(barcode_batch), 0
        missing_barcodes = []
        logger.info(f"Starting processing of {total_records:,} barcodes")
        for i, barcode in enumerate(barcode_batch):
            if i > 0 and not i % 100:
                logger.info(f"Barcode count {i:,}")
            result = check_update_item(barcode, folio_client, stat_code_lookup)
            if "error" in result:
                if result['error'].startswith("not found barcode"):
                    missing_barcodes.append(barcode)
                logging.error(f"{i}: {barcode} error: {result['error']}")
                # Error count, in the future we could expand reporting out
                errors += 1
        if len(missing_barcodes) > 0:
            missing_barcode_file_path = save_missing_barcodes(missing_barcodes, sdr_dir="/opt/airflow/sdr-files")
            task_instance.xcom_push(key="missing_barcode_file", value=missing_barcode_file_path)
            logger.info(f"Saved {len(missing_barcodes):,} to file {missing_barcode_file_path}")
        logger.info(f"Finished loading {total_records:,} number of errors: {errors:,}")

    @task
    def combine_missing_barcode_files(processed_batches):
        concat_missing_barcodes(processed_batches)



    @task
    def remove_csv_file(**kwargs):
        task_instance = kwargs["ti"]
        csv_file = task_instance.xcom_pull(
            task_ids="setup_stat_code_management", key="csv_file"
        )
        delete_barcode_csv(csv_file)

    @task
    def sdr_pull_items(**kwargs):
        logger.info("SDR API pull not available")

    barcode_batches = get_batches_from_csv()
    remove_barcode_csv = remove_csv_file()
    sdr_barcodes = sdr_pull_items()
    stat_code_lookup = get_stat_code_lookup()
    finished = EmptyOperator(
        task_id="finished-sdr-tag-management",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    batches_processed = process_barcode_batch.expand(batch=barcode_batches)
    consolidate_missing_barcodes = combine_missing_barcode_files(batches_processed.output)
    setup = setup_dag()

    setup >> [barcode_batches, sdr_barcodes]
    consolidate_missing_barcodes >> remove_barcode_csv
    remove_barcode_csv >> finished
    sdr_barcodes >> finished


manage_sdr_stat_codes()
