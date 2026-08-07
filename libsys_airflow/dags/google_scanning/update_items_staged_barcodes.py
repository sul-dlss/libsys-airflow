import logging

from datetime import datetime

from airflow.sdk import dag, task, get_current_context, Variable
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.empty import EmptyOperator

from libsys_airflow.plugins.google_scanning.helpers import (
    get_folio_uuids,
    process_barcode,
    read_staged_barcode_files,
    write_status_json,
)
from libsys_airflow.plugins.shared.folio_client import folio_client

logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    catchup=False,
    start_date=datetime(2026, 8, 6),
    tags=["google_scanning", "folio"],
    max_active_runs=1,
)
def stage_cart_items():

    @task
    def setup(**kwargs) -> dict:
        context = get_current_context()
        params = context.get("params", {})
        if "staged_file_path" not in params:
            raise ValueError("Missing staged_file_path")
        if "cart_name" not in params:
            raise ValueError("Missing cart name")
        return {
            "cart_name": params["cart_name"],
            "staged_file_path": params["staged_file_path"],
        }

    @task
    def get_and_batch_barcodes(staged_file_path: str) -> list:
        all_barcodes = read_staged_barcode_files(staged_file_path)
        total_barcodes = len(all_barcodes)
        batches = []
        # Creates up to 10 batches of barcodes
        batch_size = int(total_barcodes / 10) if total_barcodes > 10 else 10
        logger.info(f"Total {total_barcodes}")
        for i in range(0, total_barcodes, batch_size):
            batch = all_barcodes[i : i + batch_size]
            batches.append(batch)
        get_current_context()["ti"].xcom_push(key="total", value=total_barcodes)
        return batches

    @task
    def folio_uuids() -> dict:
        folio_api_client = folio_client()
        temp_location_code = Variable.get(
            "GOOGLE_SCANNING_CAMPUS_TEMP_LOCATION_ID", "GRE-GOOGLE-SCANNING-ONCAMPUS"
        )
        temp_location_id, digi_sent_id, note_type_id = get_folio_uuids(
            folio_api_client, temp_location_code
        )

        return {
            "digi_sent_id": digi_sent_id,
            "note_type_id": note_type_id,
            "temp_location_id": temp_location_id,
        }

    @task(max_active_tis_per_dagrun=3)
    def process_barcodes_batch(**kwargs) -> dict:
        barcodes_batch: list = kwargs["batch"]
        temp_location_id: str = kwargs["temp_location_id"]
        digi_sent_id: str = kwargs["digi_sent_id"]
        note_type_id: str = kwargs["note_type_id"]

        folio_api_client = folio_client()

        succeeded, missing, errors = 0, [], []
        total_barcodes = len(barcodes_batch)
        logger.info(f"Starting processing {total_barcodes:,} barcodes")

        for i, barcode in enumerate(barcodes_batch):
            result = process_barcode(
                barcode=barcode,
                folio_client=folio_api_client,
                temp_location_id=temp_location_id,
                digi_sent_id=digi_sent_id,
                note_type_id=note_type_id,
            )
            if len(result) < 1:
                succeeded += 1
            elif "missing" in result:
                missing.append(barcode)
            else:
                errors.append(result)

            if i > 0 and not i % 100:
                logger.info(f"Processed {i:,} barcodes")

        logger.info(
            f"Finished processing {total_barcodes}, updated {succeeded:,}, total errors {len(errors)}"
        )
        return {
            "errors": errors,
            "missing": missing,
            "successful_updates": succeeded,
        }

    @task
    def generate_status_json(init_params: dict, batch_results: list):
        total_barcodes = get_current_context()["ti"].xcom_pull(
            task_ids="get_and_batch_barcodes", key="total"
        )
        update_results = {
            "successful_updates": sum(
                result["successful_updates"] for result in batch_results
            ),
            "missing": [
                barcode for result in batch_results for barcode in result["missing"]
            ],
            "errors": [error for result in batch_results for error in result["errors"]],
        }
        if write_status_json(init_params, total_barcodes, update_results) is False:
            raise AirflowException("Failed to write status.json")

    init_params = setup()

    folio_ids = folio_uuids()

    barcode_batches = get_and_batch_barcodes(init_params["staged_file_path"])

    update_items_result = process_barcodes_batch.partial(
        digi_sent_id=folio_ids["digi_sent_id"],
        note_type_id=folio_ids["note_type_id"],
        temp_location_id=folio_ids["temp_location_id"],
    ).expand(batch=barcode_batches)

    generate_status_json(init_params, update_items_result) >> EmptyOperator(
        task_id="end"
    )


stage_cart_items()
