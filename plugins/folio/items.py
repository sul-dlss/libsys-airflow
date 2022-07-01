import json
import logging

from folio_migration_tools.migration_tasks.items_transformer import ItemsTransformer
from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID

from plugins.folio.helpers import post_to_okapi, setup_data_logging

logger = logging.getLogger(__name__)


def _add_hrid(okapi_url: str, holdings_path: str, items_path: str):
    """Adds an HRID based on Holdings formerIds"""

    # Initializes Holdings lookup and counter
    holdings_keys = {}

    with open(holdings_path) as fo:
        for line in fo.readlines():
            holdings_record = json.loads(line)
            holdings_keys[holdings_record["id"]] = {
                "formerId": holdings_record["formerIds"][0],
                "counter": 0,
            }

    items = []
    with open(items_path) as fo:
        for line in fo.readlines():
            item = json.loads(line)
            holding = holdings_keys[item["holdingsRecordId"]]
            former_id = holding["formerId"]
            holding["counter"] = holding["counter"] + 1
            hrid_prefix = former_id[:1] + "i" + former_id[1:]
            item["hrid"] = f"{hrid_prefix}_{holding['counter']}"
            if "barcode" in item:
                id_seed = item["barcode"]
            else:
                id_seed = item["hrid"]
            item["id"] = str(
                FolioUUID(
                    okapi_url,
                    FOLIONamespaces.items,
                    id_seed,
                )
            )
            items.append(item)

    with open(items_path, "w+") as write_output:
        for item in items:
            write_output.write(f"{json.dumps(item)}\n")


def post_folio_items_records(**kwargs):
    """Creates/overlays Items records in FOLIO"""
    dag = kwargs["dag_run"]

    batch_size = int(kwargs.get("MAX_ENTITIES", 1000))
    job_number = kwargs.get("job")

    with open(f"/tmp/items-{dag.run_id}-{job_number}.json") as fo:
        items_records = json.load(fo)

    for i in range(0, len(items_records), batch_size):
        items_batch = items_records[i:i + batch_size]
        logger.info(f"Posting {len(items_batch)} in batch {i/batch_size}")
        post_to_okapi(
            token=kwargs["task_instance"].xcom_pull(
                key="return_value", task_ids="post-to-folio.folio_login"
            ),
            records=items_batch,
            endpoint="/item-storage/batch/synchronous?upsert=true",
            payload_key="items",
            **kwargs,
        )


def run_items_transformer(*args, **kwargs) -> bool:
    """Runs item tranformer"""
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]

    library_config = kwargs["library_config"]
    library_config.iteration_identifier = dag.run_id

    items_stem = kwargs["items_stem"]

    item_config = ItemsTransformer.TaskConfiguration(
        name="items-transformer",
        migration_task_type="ItemsTransformer",
        hrid_handling="preserve001",
        files=[{"file_name": f"{items_stem}.notes.tsv", "suppress": False}],
        items_mapping_file_name="item_mapping.json",
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        material_types_map_file_name="material_types.tsv",
        loan_types_map_file_name="loan_types.tsv",
        statistical_codes_map_file_name="statcodes.tsv",
        item_statuses_map_file_name="item_statuses.tsv",
        call_number_type_map_file_name="call_number_type_mapping.tsv",
    )

    items_transformer = ItemsTransformer(item_config, library_config, use_logging=False)

    setup_data_logging(items_transformer)

    items_transformer.do_work()

    items_transformer.wrap_up()

    _add_hrid(
        items_transformer.folio_client.okapi_url,
        f"{airflow}/migration/results/folio_holdings_{dag.run_id}_holdings-transformer.json",
        f"{airflow}/migration/results/folio_items_{dag.run_id}_items-transformer.json",
    )
