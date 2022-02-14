import csv

from airflow.models import Variable

from migration_tools.migration_tasks.holdings_csv_transformer import (
    HoldingsCsvTransformer,
)


def run_holdings_tranformer(*args, **kwargs):
    dag = kwargs["dag_run"]
    library_config = kwargs["library_config"]
    library_config.iteration_identifier = dag.run_id
    task_instance = kwargs["task_instance"]

    holdings_stem = task_instance.xcom_pull(key="return_value", task_ids="move_transform_files")

    holdings_configuration = HoldingsCsvTransformer.TaskConfiguration(
        name="holdings-transformer",
        migration_task_type="HoldingsMarcTransformer",
        use_tenant_mapping_rules=False,
        hrid_handling="default",
        files=[{ "file_name": f"{holdings_stem}.tsv", "suppress": False}],
        create_source_records=False,
        call_number_type_map_file_name="call_number_type_mapping.tsv",
        holdings_map_file_name=Variable.get("HOLDINGS_MAP_FILE_NAME"),
        location_map_file_name="locations.tsv",
        default_call_number_type_name="Library of Congress classification",
        default_holdings_type_id="03c9c400-b9e3-4a07-ac0e-05ab470233ed",
    )

    holdings_transformer = HoldingsCsvTransformer(
        holdings_configuration, library_config, use_logging=False
    )

    holdings_transformer.do_work()

    holdings_transformer.wrap_up()