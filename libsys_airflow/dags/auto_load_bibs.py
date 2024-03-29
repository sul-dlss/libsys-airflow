import logging
from datetime import datetime
import pathlib

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=datetime(2022, 8, 11),
    catchup=False,
    tags=["folio", "bib_import"],
    max_active_runs=1,
)
def auto_bib_loads(**kwargs):
    """
    ## Automatic BIB Records Loading and Migration
    Extracts MARC 21 records with accompanying TSV files from a directory
    and launches new symphony_marc_import DAG runs with the files
    """

    @task()
    def create_bib_loads():
        """
        ### Creates BIB Record Loads
        Iterates through a directory of MARC21 and TSV files
        """
        context = get_current_context()
        params = context.get("params")

        files_directory = params.get("directory", "/opt/airflow/symphony/")

        logger.info(f"Generating Record Loads from {files_directory}")

        files_path = pathlib.Path(files_directory)

        bib_record_groups = []
        for marc_file in files_path.glob("*[0-9].mrc"):
            record_group = {
                "marc": str(marc_file),
                "tsv": [],
                "tsv-base": None,
                "tsv-dates": None,
                "tsv-bwchild": None,
            }
            marc_file_parts = marc_file.stem.split(".")
            mhld_file = files_path / ".".join([marc_file_parts[0], "mhld", "mrc"])
            if mhld_file.exists():
                record_group["mhld"] = str(mhld_file)
                logger.info(
                    f"MHLD file path is {mhld_file} exists {mhld_file.exists()}"
                )
            for tsv_file in files_path.glob(f"{marc_file.stem}*.tsv"):
                tsv_file_str = str(tsv_file)
                if tsv_file.name == f"{marc_file.stem}.tsv":
                    record_group["tsv-base"] = tsv_file_str
                elif tsv_file.name == f"{marc_file.stem}.dates.tsv":
                    record_group["tsv-dates"] = tsv_file_str
                elif tsv_file.name.endswith("bwchild.tsv"):
                    record_group["tsv-bwchild"] = tsv_file_str
                elif tsv_file.name.endswith("holdingsnote.tsv"):
                    record_group["tsv-holdingsnotes"] = tsv_file_str
                elif tsv_file.name.endswith("instatcode.tsv"):
                    record_group["tsv-instatcode"] = tsv_file_str
                else:
                    record_group["tsv"].append(tsv_file_str)
            bib_record_groups.append(record_group)
            logger.info(f"{marc_file.name} with {len(record_group['tsv']):,} tsv files")

        logger.info(f"Total {len(bib_record_groups):,} ")
        return bib_record_groups

    @task()
    def launch_ol_management(**kwargs):
        """
        ### Launches inventory_ol_manage DAG
        """
        record_loads = kwargs.get("bib_rec_groups", [])
        if len(record_loads) > 0:
            TriggerDagRunOperator(
                task_id="ol-management",
                trigger_dag_id="optimistic_locking_management",
            ).execute(kwargs)
        return record_loads

    @task()
    def launch_bib_imports(**kwargs):
        """
        ### Launches multiples DAG runs of symphony_marc_import
        """
        bib_record_groups = kwargs.get("bib_rec_groups", [])
        for i, group in enumerate(bib_record_groups):
            TriggerDagRunOperator(
                task_id=f"symphony-marc-import-{i}",
                trigger_dag_id="symphony_marc_import",
                conf={"record_group": group},
            ).execute(kwargs)

    record_groups = create_bib_loads()
    ol_groups = launch_ol_management(bib_rec_groups=record_groups)
    launch_bib_imports(bib_rec_groups=ol_groups)


auto_bib_loader = auto_bib_loads()
