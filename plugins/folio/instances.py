import json
import logging
import pandas as pd

from folio_migration_tools.migration_tasks.bibs_transformer import BibsTransformer

from plugins.folio.helpers import post_to_okapi, setup_data_logging

logger = logging.getLogger(__name__)


def _adjust_records(bibs_transformer: BibsTransformer, tsv_dates: str):
    dates_df = pd.read_csv(tsv_dates, sep="\t", dtype={"CATKEY": str, "CREATED_DATE": str, "CATALOGED_DATE": str})
    records = []
    with open(bibs_transformer.processor.results_file.name) as fo:
        for row in fo.readlines():
            record = json.loads(row)
            record["_version"] = 1  # for handling optimistic locking
            ckey = record["hrid"].removeprefix("a")
            matched_row = dates_df.loc[dates_df["CATKEY"] == ckey]
            if matched_row["CATALOGED_DATE"].values[0] != "0":
                record["catalogedDate"] = matched_row["CATALOGED_DATE"].values[0]
            records.append(record)
    with open(bibs_transformer.processor.results_file.name, "w+") as fo:
        for record in records:
            fo.write(f"{json.dumps(record)}\n")


def post_folio_instance_records(**kwargs):
    """Creates new records in FOLIO"""
    dag = kwargs["dag_run"]

    batch_size = int(kwargs.get("MAX_ENTITIES", 1000))
    job_number = kwargs.get("job")

    with open(f"/tmp/instances-{dag.run_id}-{job_number}.json") as fo:
        instance_records = json.load(fo)

    for i in range(0, len(instance_records), batch_size):
        instance_batch = instance_records[i: i + batch_size]
        logger.info(f"Posting {len(instance_batch)} in batch {i/batch_size}")
        post_to_okapi(
            token=kwargs["task_instance"].xcom_pull(
                key="return_value", task_ids="post-to-folio.folio_login"
            ),
            records=instance_batch,
            endpoint="/instance-storage/batch/synchronous?upsert=true",
            payload_key="instances",
            **kwargs,
        )


def run_bibs_transformer(*args, **kwargs):
    dag = kwargs["dag_run"]

    library_config = kwargs["library_config"]

    marc_stem = kwargs["marc_stem"]

    tsv_dates = kwargs["dates_tsv"]

    library_config.iteration_identifier = dag.run_id

    bibs_configuration = BibsTransformer.TaskConfiguration(
        name="bibs-transformer",
        migration_task_type="BibsTransformer",
        library_config=library_config,
        hrid_handling="preserve001",
        files=[{"file_name": f"{marc_stem}.mrc", "suppress": False}],
        ils_flavour="tag001",
    )

    bibs_transformer = BibsTransformer(
        bibs_configuration, library_config, use_logging=False
    )

    setup_data_logging(bibs_transformer)

    logger.info(f"Starting bibs_tranfers work for {marc_stem}.mrc")

    bibs_transformer.do_work()

    _adjust_records(bibs_transformer, tsv_dates)

    bibs_transformer.wrap_up()
