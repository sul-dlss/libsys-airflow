import json
import logging
import pathlib
import re
import shutil

import numpy as np
import pandas as pd
import pymarc
import requests


from airflow.models import Variable
from folioclient import FolioClient
from folio_migration_tools.migration_tasks.migration_task_base import LevelFilter

logger = logging.getLogger(__name__)


def archive_artifacts(*args, **kwargs):
    """Archives JSON Instances, Items, and Holdings"""
    dag = kwargs["dag_run"]

    airflow = kwargs.get("airflow", "/opt/airflow")
    tmp = kwargs.get("tmp_dir", "/tmp")

    airflow_path = pathlib.Path(airflow)
    tmp_path = pathlib.Path(tmp)

    airflow_results = airflow_path / "migration/results"
    archive_directory = airflow_path / "migration/archive"

    if not archive_directory.exists():
        archive_directory.mkdir()

    for tmp_file in tmp_path.glob("*.json"):
        try:
            tmp_file.unlink()
        except OSError as err:
            logger.info(f"Cannot remove {tmp_file}: {err}")

    for artifact in airflow_results.glob(f"*{dag.run_id}*.json"):

        target = archive_directory / artifact.name

        shutil.move(artifact, target)
        logger.info("Moved {artifact} to {target}")


def move_marc_files_check_tsv(*args, **kwargs) -> str:
    """Moves MARC files to migration/data/instances, sets XCOM
    if tsv is present"""
    task_instance = kwargs["task_instance"]

    airflow = kwargs.get("airflow", "/opt/airflow")
    source_directory = kwargs["source"]

    marc_path = next(pathlib.Path(f"{airflow}/{source_directory}/").glob("*.*rc"))
    if not marc_path.exists():
        raise ValueError(f"MARC Path {marc_path} does not exist")

    marc_target = pathlib.Path(f"{airflow}/migration/data/instances/{marc_path.name}")
    shutil.move(marc_path, marc_target)

    return marc_path.stem


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


full_text_check = re.compile(r"(table of contents|abstract|description|sample text)", re.IGNORECASE)


def _add_electronic_holdings(field856: pymarc.Field) -> bool:
    if field856.indicator2.startswith("1"):
        subfield_z = field856.get_subfields("z")
        subfield_3 = field856.get_subfields("3")
        subfield_all = " ".join(subfield_z + subfield_3)
        if full_text_check.match(subfield_all):
            return False
    return True

def _get_library(fields596: list) -> str:
    # Default is SUL library
    library = "SUL"
    # Use first 596 field
    subfield_a = fields596[0]['a']
    if "24" in subfield_a:
        library = "LAW"
    if "25" in subfield_a or "27" in subfield_a:
        library = "HOOVER"
    if "28" in subfield_a:
        library = "BUSINESS"
    return library


def _query_for_relationships(folio_client=None) -> dict:
    if folio_client is None:
        folio_client = FolioClient(
            Variable.get("OKAPI_URL"),
            "sul",
            Variable.get("FOLIO_USER"),
            Variable.get("FOLIO_PASSWORD")
        )

    relationships = {}
    for row in folio_client.electronic_access_relationships:
        if row['name'] == "Resource":
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


vendor_id_re = re.compile(r"\w{2,2}4")


def _extract_856s(**kwargs) -> list:
    catkey = kwargs['catkey']
    fields = kwargs['fields']
    library = kwargs['library']
    relationships = kwargs['relationships']
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
        row["HOMELOCATION"] = "INTERNET"
        if row["URI"].startswith("https://purl.stanford"):
            row["LIBRARY"] = "SUL-SDR"
            row["HOMELOCATION"] = "SUL-SDR"
        else:
            row["LIBRARY"] = library
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


def process_marc(*args, **kwargs):
    marc_stem = kwargs["marc_stem"]
    task_instance = kwargs["task_instance"]
    source_directory = kwargs["source"]
    airflow = kwargs.get("airflow", "/opt/airflow")

    marc_path = pathlib.Path(f"{airflow}/migration/data/instances/{marc_stem}.mrc")
    marc_reader = pymarc.MARCReader(marc_path.read_bytes())

    relationship_ids = _query_for_relationships(folio_client=kwargs.get("folio_client"))

    marc_records = []
    electronic_holdings = []
    for record in marc_reader:
        catkey = _move_001_to_035(record)
        electronic_holdings.extend(
            _extract_856s(
                catkey=catkey,
                fields=record.get_fields("856"),
                relationships=relationship_ids,
                library=_get_library(record.get_fields("596"))))
        marc_records.append(record)
        count = len(marc_records)
        if not count % 10_000:
            logger.info(f"Processed {count} MARC records")

    marc_only = True

    # Checks for TSV file and sets XCOM marc_only if not present
    tsv_path = pathlib.Path(f"{airflow}/{source_directory}/{marc_path.stem}.tsv")

    if tsv_path.exists():
        marc_only = False

    if len(electronic_holdings) > 0:
        electronic_holdings_df = pd.DataFrame(electronic_holdings)
        electronic_holdings_df.to_csv(
            f"{airflow}/migration/data/items/{marc_stem}.electronic.tsv",
            sep="\t",
            index=False,
        )
        marc_only = False
        logger.info(f"Creating {len(electronic_holdings):,} Electronic Holdings")

 
    task_instance.xcom_push(key="marc_only", value=marc_only)

    with open(marc_path.absolute(), "wb+") as fo:
        marc_writer = pymarc.MARCWriter(fo)
        for i, record in enumerate(marc_records):
            marc_writer.write(record)
            if not i % 10000:
                logger.info(f"Writing record {i}")


def _save_error_record_ids(**kwargs):
    dag = kwargs["dag_run"]
    records = kwargs["records"]
    endpoint = kwargs["endpoint"]
    error_code = kwargs["error_code"]
    airflow = kwargs.get("airflow", pathlib.Path("/opt/airflow/"))

    record_base = endpoint.split("/")[1]

    error_filepath = (
        airflow
        / "migration/results"
        / f"errors-{record_base}-{error_code}-{dag.run_id}.json"
    )

    with error_filepath.open("a") as error_file:
        for rec in records:
            error_file.write(json.dumps(rec))
            error_file.write("\n")


def post_to_okapi(**kwargs) -> bool:
    endpoint = kwargs.get("endpoint")
    jwt = kwargs["token"]

    records = kwargs["records"]
    payload_key = kwargs["payload_key"]

    tenant = "sul"
    okapi_url = Variable.get("OKAPI_URL")

    okapi_instance_url = f"{okapi_url}{endpoint}"

    headers = {
        "Content-type": "application/json",
        "user-agent": "FolioAirflow",
        "x-okapi-token": jwt,
        "x-okapi-tenant": tenant,
    }

    payload = {payload_key: records}

    new_record_result = requests.post(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    logger.info(
        f"Result status code {new_record_result.status_code} for {len(records)} records"  # noqa
    )

    if new_record_result.status_code > 399:
        logger.error(new_record_result.text)
        _save_error_record_ids(error_code=new_record_result.status_code, **kwargs)


def process_records(*args, **kwargs) -> list:
    """Function creates valid json from file of FOLIO objects"""
    prefix = kwargs.get("prefix")
    dag = kwargs["dag_run"]

    pattern = f"{prefix}*{dag.run_id}*.json"

    out_filename = f"{kwargs.get('out_filename')}-{dag.run_id}"

    total_jobs = int(kwargs.get("jobs"))

    airflow = kwargs.get("airflow", "/opt/airflow")
    tmp = kwargs.get("tmp", "/tmp")

    records = []
    for file in pathlib.Path(f"{airflow}/migration/results").glob(pattern):
        with open(file) as fo:
            records.extend([json.loads(i) for i in fo.readlines()])

    shard_size = int(len(records) / total_jobs)

    for i in range(total_jobs):
        start = i * shard_size
        end = shard_size * (i + 1)
        if i == total_jobs - 1:
            end = len(records)
        tmp_out_path = f"{tmp}/{out_filename}-{i}.json"
        logger.info(f"Start {start} End {end} for {tmp_out_path}")
        with open(tmp_out_path, "w+") as fo:
            json.dump(records[start:end], fo)

    return len(records)


def setup_data_logging(transformer):
    def transformer_data_issues(transformer, message, *args, **kwargs):
        transformer._log(DATA_ISSUE_LVL_NUM, message, args, **kwargs)

    # Set DATA_ISSUE logging levels
    DATA_ISSUE_LVL_NUM = 26
    logging.addLevelName(DATA_ISSUE_LVL_NUM, "DATA_ISSUES")
    logging.Logger.data_issues = transformer_data_issues

    data_issue_file_formatter = logging.Formatter("%(message)s")
    data_issue_file_handler = logging.FileHandler(
        filename=str(transformer.folder_structure.data_issue_file_path),
    )
    data_issue_file_handler.addFilter(LevelFilter(26))
    data_issue_file_handler.setFormatter(data_issue_file_formatter)
    data_issue_file_handler.setLevel(26)
    logging.getLogger().addHandler(data_issue_file_handler)


def _apply_transforms(df, column_transforms):
    for transform in column_transforms:
        column = transform[0]
        if column in df:
            function = transform[1]
            df[column] = df[column].apply(function)
    return df


def _merge_notes_into_base(base_df: pd.DataFrame, note_df: pd.DataFrame):
    def _populate_column(barcode):
        matched_series = note_df.loc[note_df["BARCODE"] == barcode]
        if len(matched_series) > 0:
            return matched_series[note_type].values[0]
        else:
            return np.nan

    # Extract name of non-barcode column
    note_columns = list(note_df.columns)
    note_columns.pop(note_columns.index("BARCODE"))
    note_type = note_columns[0]
    base_df[note_type] = base_df["BARCODE"].apply(_populate_column)
    return base_df


def _processes_tsv(tsv_base, tsv_notes, airflow, column_transforms):
    items_dir = pathlib.Path(f"{airflow}/migration/data/items/")

    tsv_base_df = pd.read_csv(tsv_base, sep="\t", dtype=object)
    tsv_base_df = _apply_transforms(tsv_base_df, column_transforms)
    new_tsv_base_path = items_dir / tsv_base.name

    tsv_base_df.to_csv(new_tsv_base_path, sep="\t", index=False)
    tsv_base.unlink()

    # Iterate on tsv notes and merge into the tsv base DF based on barcode
    for tsv_notes_path in tsv_notes:
        note_df = pd.read_csv(tsv_notes_path, sep="\t", dtype=object)
        note_df = _apply_transforms(note_df, column_transforms)
        tsv_base_df = _merge_notes_into_base(tsv_base_df, note_df)
        logging.info(f"Merged {len(note_df)} notes into items tsv")
        tsv_notes_path.unlink()

    tsv_notes_name_parts = tsv_base.name.split(".")
    tsv_notes_name_parts.insert(-1, "notes")

    tsv_notes_name = ".".join(tsv_notes_name_parts)

    new_tsv_notes_path = pathlib.Path(
        f"{airflow}/migration/data/items/{tsv_notes_name}"
    )
    tsv_base_df.to_csv(new_tsv_notes_path, sep="\t", index=False)

    return new_tsv_notes_path.name


def _get_tsv_notes(tsv_stem, airflow, source_directory):

    return [
        path
        for path in pathlib.Path(f"{airflow}/{source_directory}/").glob(
            f"{tsv_stem}.*.tsv"
        )
    ]


def transform_move_tsvs(*args, **kwargs):
    airflow = kwargs.get("airflow", "/opt/airflow")
    column_transforms = kwargs.get("column_transforms", [])
    tsv_stem = kwargs["tsv_stem"]
    source_directory = kwargs["source"]

    tsv_base = pathlib.Path(f"{airflow}/{source_directory}/{tsv_stem}.tsv")

    if not tsv_base.exists():
        logging.info(f"{tsv_base} does not exist for workflow")
        return

    tsv_notes = _get_tsv_notes(tsv_stem, airflow, source_directory)

    notes_path_name = _processes_tsv(tsv_base, tsv_notes, airflow, column_transforms)

    return [tsv_base.name, notes_path_name]
