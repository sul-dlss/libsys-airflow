import json
import logging

from migration_tools.migration_tasks.items_transformer import ItemsTransformer

from plugins.folio.helpers import post_to_okapi

logger = logging.getLogger(__name__)


def post_folio_items_records(**kwargs):
    """Creates/overlays Items records in FOLIO"""
    dag = kwargs["dag_run"]

    batch_size = kwargs.get("MAX_ENTITIES", 1000)
    job_number = kwargs.get("job")

    with open(f"/tmp/items-{dag.run_id}-{job_number}.json") as fo:
        items_records = json.load(fo)

    for i in range(0, len(items_records), batch_size):
        items_batch = items_records[i : i + batch_size]
        logger.info(f"Posting {i} to {i/batch_size} items records")
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
    dag = kwargs["dag_run"]
    library_config = kwargs["library_config"]
    library_config.iteration_identifier = dag.run_id

    items_stem = kwargs["items_stem"]

    item_config = ItemsTransformer.TaskConfiguration(
        name="bibs-transformer",
        migration_task_type="ItemsTransformer",
        hrid_handling="default",
        files=[{"file_name": f"{items_stem}.tsv", "suppress": False}],
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

    items_transformer.do_work()

    items_transformer.wrap_up()

    return True
