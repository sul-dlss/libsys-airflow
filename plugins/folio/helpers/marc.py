import logging
import json
import pathlib
import re
import shutil

import pandas as pd
import pymarc

from pathlib import Path

from airflow.models import Variable

from folioclient import FolioClient
from folio_migration_tools.migration_tasks.batch_poster import BatchPoster


logger = logging.getLogger(__name__)


full_text_check = re.compile(
    r"(table of contents|abstract|description|sample text)", re.IGNORECASE
)
vendor_id_re = re.compile(r"\w{2,2}4")


def _add_electronic_holdings(field856: pymarc.Field) -> bool:
    if field856.indicator2.startswith("1"):
        subfield_z = field856.get_subfields("z")
        subfield_3 = field856.get_subfields("3")
        subfield_all = " ".join(subfield_z + subfield_3)
        if full_text_check.match(subfield_all):
            return False
    return True


def _extract_856s(catkey: str, fields: list, relationships: dict) -> list:
    properties_names = [
        "CATKEY",
        "HOMELOCATION",
        "LIBRARY",
        "LINK_TEXT",
        "MAT_SPEC",
        "PUBLIC_NOTE",
        "RELATIONSHIP",
        "URI",
        "VENDOR_CODE",
        "NOTE",
    ]
    output = []
    for field856 in fields:
        if not _add_electronic_holdings(field856):
            continue
        row = {}
        for field in properties_names:
            row[field] = None
        row["CATKEY"] = catkey
        row["URI"] = "".join(field856.get_subfields("u"))
        if row["URI"].startswith("https://purl.stanford"):
            row["HOMELOCATION"] = "SUL-SDR"
            row["LIBRARY"] = "SUL-SDR"
        else:
            row["HOMELOCATION"] = "INTERNET"
            row["LIBRARY"] = "SUL"
        material_type = field856.get_subfields("3")
        if len(material_type) > 0:
            row["MAT_SPEC"] = " ".join(material_type)
        if link_text := field856.get_subfields("y"):
            row["LINK_TEXT"] = " ".join(link_text)
        if public_note := field856.get_subfields("z"):
            row["PUBLIC_NOTE"] = " ".join(public_note)
        if all_x_subfields := field856.get_subfields("x"):
            # Checks second to last x for vendor code
            if len(all_x_subfields) >= 2 and vendor_id_re.search(all_x_subfields[-2]):
                row["VENDOR_CODE"] = all_x_subfields.pop(-2)
            # Adds remaining x subfield values to note
            row["NOTE"] = "|".join(all_x_subfields)
        if field856.indicator2 in relationships:
            row["RELATIONSHIP"] = relationships[field856.indicator2]
        else:
            row["RELATIONSHIP"] = relationships["_"]
        output.append(row)
    return output


def _move_001_to_035(record: pymarc.Record) -> str:
    all001 = record.get_fields("001")

    if len(all001) < 1:
        return

    catkey = all001[0].data

    if len(all001) > 1:
        for field001 in all001[1:]:
            field035 = pymarc.Field(
                tag="035", indicators=["", ""], subfields=["a", field001.data]
            )
            record.add_field(field035)
            record.remove_field(field001)

    return catkey


def _query_for_relationships(folio_client=None) -> dict:
    if folio_client is None:
        folio_client = FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD"),
        )

    relationships = {}
    for row in folio_client.electronic_access_relationships:
        if row["name"] == "Resource":
            relationships["0"] = row["id"]
        if row["name"] == "Version of resource":
            relationships["1"] = row["id"]
        if row["name"] == "Related resource":
            relationships["2"] = row["id"]
        if row["name"] == "No display constant generated":
            relationships["8"] = row["id"]
        if row["name"] == "No information provided":
            relationships["_"] = row["id"]
    return relationships


def marc_only(*args, **kwargs):
    task_instance = kwargs["task_instance"]
    tsv_files = task_instance.xcom_pull(task_ids="bib-files-group", key="tsv-files")
    tsv_base = task_instance.xcom_pull(task_ids="bib-files-group", key="tsv-base")
    all_next_task_id = kwargs.get("default_task")
    marc_only_task_id = kwargs.get("marc_only_task")

    if len(tsv_files) < 1 and tsv_base is None:
        return marc_only_task_id
    return all_next_task_id


def move_marc_files(*args, **kwargs) -> str:
    """Moves MARC files to migration/data/instances"""
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]

    task_instance = kwargs["task_instance"]

    marc_filepath = task_instance.xcom_pull(task_ids="bib-files-group", key="marc-file")

    marc_path = pathlib.Path(marc_filepath)
    marc_target = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/source_data/instances/{marc_path.name}"
    )

    shutil.move(marc_path, marc_target)
    logger.info(f"{marc_path} moved to {marc_target}")

    # Moves MHLDs to Holdings
    mhld_filepath = task_instance.xcom_pull(task_ids="bib-files-group", key="mhld-file")
    if mhld_filepath is not None:
        mhld_path = pathlib.Path(mhld_filepath)
        if mhld_path.exists():
            mhld_target = pathlib.Path(
                f"{airflow}/migration/iterations/{dag.run_id}/source_data/holdings/{mhld_path.name}"
            )

            shutil.move(mhld_path, mhld_target)
            logger.info(f"{mhld_path} moved to {mhld_target}")

    return marc_path.stem


def post_marc_to_srs(*args, **kwargs):
    dag = kwargs.get("dag_run")
    srs_filepath = kwargs.get("srs_file")

    task_config = BatchPoster.TaskConfiguration(
        name="marc-to-srs-batch-poster",
        migration_task_type="BatchPoster",
        object_type="SRS",
        files=[{"file_name": srs_filepath}],
        batch_size=kwargs.get("MAX_ENTITIES", 1000),
    )

    library_config = kwargs["library_config"]
    library_config.iteration_identifier = dag.run_id

    srs_batch_poster = BatchPoster(task_config, library_config, use_logging=False)

    srs_batch_poster.do_work()

    srs_batch_poster.wrap_up()

    logger.info("Finished posting MARC json to SRS")


def process(*args, **kwargs):
    marc_stem = kwargs["marc_stem"]
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]

    marc_path = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/source_data/instances/{marc_stem}.mrc"
    )
    marc_reader = pymarc.MARCReader(marc_path.read_bytes())

    relationship_ids = _query_for_relationships(folio_client=kwargs.get("folio_client"))

    marc_records = []
    electronic_holdings = []
    for record in marc_reader:
        catkey = _move_001_to_035(record)
        electronic_holdings.extend(
            _extract_856s(catkey, record.get_fields("856"), relationship_ids)
        )
        marc_records.append(record)
        count = len(marc_records)
        if not count % 10_000:
            logger.info(f"Processed {count} MARC records")

    electronic_holdings_df = pd.DataFrame(electronic_holdings)
    electronic_holdings_df.to_csv(
        f"{airflow}/migration/iterations/{dag.run_id}/source_data/items/{marc_stem}.electronic.tsv",
        sep="\t",
        index=False,
    )

    with open(marc_path.absolute(), "wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for i, record in enumerate(marc_records):
            marc_writer.write(record)
            if not i % 10000:
                logger.info(f"Writing record {i}")




def remove_srs_json(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    srs_filename = kwargs["srs_filename"]
    dag = kwargs["dag_run"]

    srs_filedir = Path(airflow) / f"migration/iterations/{dag.run_id}/results/"

    for p in Path(srs_filedir).glob(f"{srs_filename}*"):
        p.unlink()
        logger.info(f"Removed {p}")
