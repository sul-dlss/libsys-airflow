import logging
import pathlib
import re
import shutil

import pandas as pd
import pymarc

from folio_migration_tools.migration_tasks.batch_poster import BatchPoster


logger = logging.getLogger(__name__)


full_text_check = re.compile(
    r"(table of contents|abstract|description|sample text)", re.IGNORECASE
)

vendor_id_re = re.compile(r"\w{2,2}4")

sdr_sul_re = re.compile(r"https*:\/\/purl.stanford.edu")


def _add_electronic_holdings(field: pymarc.Field) -> bool:
    if field.indicator2 in ["0", "1"]:
        subfield_z = field.get_subfields("z")
        subfield_3 = field.get_subfields("3")
        subfield_all = " ".join(subfield_z + subfield_3)
        if full_text_check.search(subfield_all):
            return False
        return True
    return False


def _extract_fields(**kwargs) -> list:
    catkey = kwargs["catkey"]
    fields = kwargs["fields"]
    library = kwargs["library"]

    properties_names = [
        "CATKEY",
        "HOMELOCATION",
        "LIBRARY",
        "MAT_SPEC",
    ]
    output = []
    for marc_field in fields:
        if _add_electronic_holdings(marc_field) is False:
            continue
        row = {}
        for field in properties_names:
            row[field] = None
        row["CATKEY"] = catkey
        uri = "".join(marc_field.get_subfields("u"))

        if sdr_sul_re.search(uri):
            row["LIBRARY"] = "SUL-SDR"
        else:
            row["LIBRARY"] = library

        row["HOMELOCATION"] = "INTERNET"

        material_type = marc_field.get_subfields("3")
        if len(material_type) > 0:
            row["MAT_SPEC"] = " ".join(material_type)
        output.append(row)
    return output


def _get_library(fields596: list) -> str:
    # Default is SUL library
    library = "SUL"
    if len(fields596) < 1:
        return library
    # Use first 596 field
    subfield_a = "".join(fields596[0].get_subfields("a"))
    if "24" in subfield_a:
        library = "LAW"
    elif "25" in subfield_a or "27" in subfield_a:
        library = "HOOVER"
    elif "28" in subfield_a:
        library = "BUSINESS"
    return library


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
    airflow = kwargs.get("airflow", "/opt/airflow")
    srs_filename = kwargs.get("srs_filename")
    iteration_id = kwargs["iteration_id"]

    task_config = BatchPoster.TaskConfiguration(
        name="marc-to-srs-batch-poster",
        migration_task_type="BatchPoster",
        object_type="SRS",
        files=[{"file_name": srs_filename}],
        batch_size=kwargs.get("MAX_ENTITIES", 1000),
    )

    iteration_dir = pathlib.Path(airflow) / f"migration/iterations/{iteration_id}"

    if not (iteration_dir / f"results/{srs_filename}").exists():
        logger.info(f"{srs_filename} does not exist, existing task")
        return

    library_config = kwargs["library_config"]

    srs_batch_poster = BatchPoster(task_config, library_config, use_logging=False)

    srs_batch_poster.do_work()

    srs_batch_poster.wrap_up()

    logger.info("Finished posting MARC json to SRS")

    return srs_filename


def process(*args, **kwargs):
    marc_stem = kwargs["marc_stem"]
    airflow = kwargs.get("airflow", "/opt/airflow")
    dag = kwargs["dag_run"]

    marc_path = pathlib.Path(
        f"{airflow}/migration/iterations/{dag.run_id}/source_data/instances/{marc_stem}.mrc"
    )
    marc_reader = pymarc.MARCReader(marc_path.read_bytes())

    marc_records = []
    electronic_holdings = []
    for record in marc_reader:
        if record is None:
            continue
        catkey = _move_001_to_035(record)
        library = _get_library(record.get_fields("596"))
        electronic_holdings.extend(
            _extract_fields(
                catkey=catkey, fields=record.get_fields("856"), library=library
            )
        )
        electronic_holdings.extend(
            _extract_fields(
                catkey=catkey,
                fields=record.get_fields("956"),
                library=library
            )
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
            if not i % 10_000 and i > 0:
                logger.info(f"Writing record {i}")
