import json
import logging

from migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)

from plugins.folio.helpers import post_to_okapi, setup_data_logging

logger = logging.getLogger(__name__)


def _add_hrid(holdings_transformer):
    mapper = holdings_transformer.mapper
    for record in holdings_transformer.holdings.values():
        num_part = str(mapper.holdings_hrid_counter).zfill(11)
        record["hrid"] = f"{mapper.holdings_hrid_prefix}{num_part}"
        mapper.holdings_hrid_counter += 1


def post_folio_holding_records(**kwargs):
    """Creates/overlays Holdings records in FOLIO"""
    dag = kwargs["dag_run"]

    tmp_location = kwargs.get("tmp_dir", "/tmp")

    batch_size = kwargs.get("MAX_ENTITIES", 1000)
    job_number = kwargs.get("job")

    with open(f"{tmp_location}/holdings-{dag.run_id}-{job_number}.json") as fo:
        holding_records = json.load(fo)

    for i in range(0, len(holding_records), batch_size):
        holdings_batch = holding_records[i:i + batch_size]
        logger.info(f"Posting {i} to {i+batch_size} holding records")
        post_to_okapi(
            token=kwargs["task_instance"].xcom_pull(
                key="return_value", task_ids="post-to-folio.folio_login"
            ),
            records=holdings_batch,
            endpoint="/holdings-storage/batch/synchronous?upsert=true",
            payload_key="holdingsRecords",
            **kwargs,
        )


def run_holdings_tranformer(*args, **kwargs):
    dag = kwargs["dag_run"]
    library_config = kwargs["library_config"]
    library_config.iteration_identifier = dag.run_id

    holdings_stem = kwargs["holdings_stem"]

    holdings_configuration = HoldingsCsvTransformer.TaskConfiguration(
        name="holdings-transformer",
        migration_task_type="HoldingsCsvTransformer",
        hrid_handling="default",
        files=[{"file_name": f"{holdings_stem}.tsv", "suppress": False}],
        create_source_records=False,
        call_number_type_map_file_name="call_number_type_mapping.tsv",
        holdings_map_file_name="holdingsrecord_mapping.json",
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    )

    holdings_transformer = HoldingsCsvTransformer(
        holdings_configuration, library_config, use_logging=False
    )

    setup_data_logging(holdings_transformer)

    holdings_transformer.do_work()

    _add_hrid(holdings_transformer)

    holdings_transformer.wrap_up()

    # Manually increment HRID holdings and save
    holdings_transformer.mapper.store_hrid_settings()
