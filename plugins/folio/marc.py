import json
import logging
import pathlib

from folio_migration_tools.migration_tasks.batch_poster import BatchPoster


def post_marc_to_srs(*args, **kwargs):
    dag = kwargs["dag_run"]

    task_config = BatchPoster.TaskConfiguration(
        name="marc-to-srs-batch-poster",
        migration_task_type="BatchPoster",
        object_type="SRS",
        file={
            "file_name": f"folio_srs_instances_{dag.run_id}_bibs-transformer.json"  # noqa
        },
        batch_size=kwargs.get("MAX_ENTITIES", 1000),
    )

    library_config = kwargs["library_config"]
    library_config.iteration_identifier = dag.run_id

    srs_batch_poster = BatchPoster(
        task_config,
        library_config,
        use_logging=False
    )

    srs_batch_poster.do_work()

    srs_batch_poster.wrap_up()

    logging.info("Finished posting MARC json to SRS")


def replace_srs_record_type(*args, **kwargs):
    dag = kwargs["dag_run"]

    airflow = kwargs.get("airflow", "/opt/airflow")
    airflow_path = pathlib.Path(airflow)

    srs_file = (
        airflow_path
        / f"migration/results/folio_srs_instances_{dag.run_id}_bibs-transformer.json"  # noqa
    )

    marc_recs = []
    with open(srs_file) as fo:
        for row in fo.readlines():
            srs_marc = json.loads(row)
            srs_marc["recordType"] = "MARC"
            marc_recs.append(srs_marc)

    with open(srs_file, "w+") as fo:
        for row in marc_recs:
            fo.write(f"{json.dumps(row)}\n")
