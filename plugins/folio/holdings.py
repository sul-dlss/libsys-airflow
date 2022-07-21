import json
import logging

from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)

from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID

from plugins.folio.helpers import post_to_okapi, setup_data_logging

logger = logging.getLogger(__name__)


def _add_identifiers(holdings_transformer: HoldingsCsvTransformer):
    # Instance CATKEY
    instance_keys = {}

    for record in holdings_transformer.holdings.values():

        instance_uuid = record["instanceId"]
        former_id = record["formerIds"][0]
        # Adds an "h" for holdings prefix
        if former_id.startswith("a"):
            former_id = former_id[:1] + "h" + former_id[1:]
        if instance_uuid in instance_keys:
            new_count = instance_keys[instance_uuid] + 1
        else:
            new_count = 1
        instance_keys[instance_uuid] = new_count
        record["hrid"] = f"{former_id}_{new_count}"

        # Adds Determinstic UUID based on CATKEY and HRID
        record["id"] = str(
            FolioUUID(
                holdings_transformer.mapper.folio_client.okapi_url,
                FOLIONamespaces.holdings,
                f"{record['formerIds'][0]}{record['hrid']}",
            )
        )

        # To handle optimistic locking
        record["_version"] = 1


def post_folio_holding_records(**kwargs):
    """Creates/overlays Holdings records in FOLIO"""
    dag = kwargs["dag_run"]

    tmp_location = kwargs.get("tmp_dir", "/tmp")

    batch_size = int(kwargs.get("MAX_ENTITIES", 1000))
    job_number = kwargs.get("job")

    with open(f"{tmp_location}/holdings-{dag.run_id}-{job_number}.json") as fo:
        holding_records = json.load(fo)

    for i in range(0, len(holding_records), batch_size):
        holdings_batch = holding_records[i: i + batch_size]
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
        hrid_handling="preserve001",
        files=[{"file_name": f"{holdings_stem}.tsv", "suppress": False}],
        create_source_records=False,
        call_number_type_map_file_name="call_number_type_mapping.tsv",
        holdings_map_file_name="holdingsrecord_mapping.json",
        location_map_file_name="locations.tsv",
        holdings_type_uuid_for_boundwiths="",
        default_call_number_type_name="Library of Congress classification",
        fallback_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    )

    holdings_transformer = HoldingsCsvTransformer(
        holdings_configuration, library_config, use_logging=False
    )

    setup_data_logging(holdings_transformer)

    holdings_transformer.mapper.ignore_legacy_identifier = True

    holdings_transformer.do_work()

    _add_identifiers(holdings_transformer)

    holdings_transformer.wrap_up()
